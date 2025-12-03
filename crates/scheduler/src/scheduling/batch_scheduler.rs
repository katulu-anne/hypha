use std::time::{Duration, SystemTime};

use hypha_messages::{
    Reference, SelectionStrategy,
    action::{
        self, AggregateAction, AggregateStatus, ExecutorAction, ExecutorStatus, TrainAction,
        TrainStatus,
    },
    progress::Metrics,
};
use hypha_network::request_response::{RequestResponseError, RequestResponseInterfaceExt};
use libp2p::PeerId;
use thiserror::Error;
use tokio::{
    sync::mpsc::{self, Sender, error::SendError},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    network::Network,
    pool::{PoolHandle, PoolStatisticsHandle},
    simulation::Simulation,
    statistics::RuntimeStatistic,
};

#[derive(Debug, Error)]
pub enum BatchSchedulerError {
    #[error("Disconnected")]
    Disconnected,
    #[error("Unregistering worker")]
    Unregister,
    #[error("Error during scheduling {0}")]
    Scheduling(String),
    #[error("Network error")]
    NetworkError(#[from] RequestResponseError),
    #[error("Send Metrics Error")]
    SendMetricsError(#[from] SendError<(PeerId, Metrics)>),
}

/// Handle action protocol requests and respond with next steps.
async fn schedule<T, S>(
    tx: Sender<(PeerId, Metrics)>,
    worker_pool: PoolStatisticsHandle<T>,
    parameter_pool: PoolHandle,
    start: std::time::Instant,
    request: (PeerId, action::ActionRequest),
) -> Result<action::ActionResponse, BatchSchedulerError>
where
    T: RuntimeStatistic + 'static,
    S: Simulation + Send + Sync + 'static,
{
    let _ = std::marker::PhantomData::<S>;
    let (peer_id, action::ActionRequest { job_id, status }) = request;
    tracing::debug!(%peer_id, ?status, %job_id, "Received action request");

    let now = SystemTime::now();
    let deadline = now + Duration::from_secs(5);
    let since_start = start.elapsed().as_millis() as u64;

    let response = match status {
        ExecutorStatus::Train(train) => match train {
            TrainStatus::Idle => ExecutorAction::Train(TrainAction::ExecuteBatch),
            TrainStatus::BatchCompleted { .. } => {
                worker_pool.update(&peer_id, since_start);
                ExecutorAction::Train(TrainAction::SendUpdate {
                    target: Reference::Peers {
                        peers: parameter_pool.members().iter().map(|w| w.peer_id).collect(),
                        strategy: SelectionStrategy::All,
                        resource: None,
                    },
                    timeout: deadline,
                })
            }
            TrainStatus::SentUpdate => ExecutorAction::Train(TrainAction::ApplyUpdate {
                source: Reference::Peers {
                    peers: parameter_pool.members().iter().map(|w| w.peer_id).collect(),
                    strategy: SelectionStrategy::All,
                    resource: None,
                },
                timeout: deadline,
            }),
            TrainStatus::AppliedUpdate { round, metrics } => {
                tx.send((peer_id, Metrics { round, metrics }))
                    .await
                    .map_err(BatchSchedulerError::from)?;

                ExecutorAction::Train(TrainAction::ExecuteBatch)
            }
            TrainStatus::Terminated => ExecutorAction::Train(TrainAction::Terminate),
            TrainStatus::Error(msg) => {
                tracing::warn!(%peer_id, error = %msg, "Worker reported error");
                ExecutorAction::Train(TrainAction::Terminate)
            }
        },
        ExecutorStatus::Aggregate(state) => match state {
            AggregateStatus::Idle => ExecutorAction::Aggregate(AggregateAction::AggregateUpdates {
                source: Reference::Peers {
                    peers: worker_pool
                        .statistics()
                        .into_iter()
                        .map(|w| w.peer_id)
                        .collect(),
                    strategy: SelectionStrategy::All,
                    resource: None,
                },
            }),
            AggregateStatus::AggregatedUpdates { .. } => {
                ExecutorAction::Aggregate(AggregateAction::BroadcastUpdate {
                    target: Reference::Peers {
                        peers: worker_pool
                            .statistics()
                            .into_iter()
                            .map(|w| w.peer_id)
                            .collect(),
                        strategy: SelectionStrategy::All,
                        resource: None,
                    },
                })
            }
            AggregateStatus::BroadcastedUpdate { metrics } => {
                if let Some(metrics) = metrics {
                    tx.send((peer_id, Metrics { round: 0, metrics }))
                        .await
                        .map_err(BatchSchedulerError::from)?;
                }

                ExecutorAction::Aggregate(AggregateAction::Idle { timeout: deadline })
            }
            AggregateStatus::Terminated => ExecutorAction::Aggregate(AggregateAction::Terminate),
            AggregateStatus::Error(msg) => {
                tracing::warn!(%peer_id, error = %msg, "Aggregator reported error");
                ExecutorAction::Aggregate(AggregateAction::Terminate)
            }
        },
    };

    Ok(action::ActionResponse {
        job_id,
        next: response,
    })
}

pub struct BatchScheduler {}

impl BatchScheduler {
    pub async fn run<T, S>(
        network: Network,
        worker_pool: PoolStatisticsHandle<T>,
        parameter_pool: PoolHandle,
        id: Uuid,
    ) -> Result<(mpsc::Receiver<(PeerId, Metrics)>, JoinHandle<()>), BatchSchedulerError>
    where
        T: RuntimeStatistic + 'static,
        S: Simulation + Send + Sync + 'static,
    {
        let _ = std::marker::PhantomData::<S>;
        let (tx, rx) = mpsc::channel(100);
        let start = std::time::Instant::now();
        let stream_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            let parameter_pool = parameter_pool.clone();
            network
                .on::<action::Codec, _>(move |req: &action::ActionRequest| {
                    matches!(
                        req,
                        action::ActionRequest{job_id, ..}
                    if &id == job_id
                    )
                })
                .into_stream()
                .await
                .map_err(BatchSchedulerError::from)?
                .respond_with_concurrent(None, move |request| {
                    let tx = tx.clone();
                    let worker_pool = worker_pool.clone();
                    let parameter_pool = parameter_pool.clone();
                    async move {
                        match schedule::<T, S>(tx, worker_pool, parameter_pool, start, request)
                            .await
                        {
                            Ok(response) => response,
                            Err(e) => {
                                tracing::warn!(error = ?e, "Error handling request");
                                action::ActionResponse {
                                    job_id: id,
                                    next: ExecutorAction::Train(TrainAction::Terminate),
                                }
                            }
                        }
                    }
                })
        });

        let handle = tokio::spawn(async move {
            if let Err(e) = stream_handle.await {
                tracing::warn!(error = ?e, "Stream handler finished with error");
            }
        });
        Ok((rx, handle))
    }
}

#[cfg(test)]
mod batch_scheduler_tests {
    use hypha_messages::{
        action::{ActionRequest, ExecutorStatus, TrainAction, TrainStatus},
        progress::Metrics,
    };
    use hypha_resources::Resources;
    use libp2p::PeerId;
    use tokio::time::Duration;
    use uuid::Uuid;

    use super::schedule;
    use crate::{
        allocator::{Allocator, AllocatorError},
        pool::{Pool, PoolConfig, PoolWithStatistics},
        scheduler_config::PriceRange,
        simulation::BasicSimulation,
        statistics::RunningMean,
        worker::Worker,
    };

    struct NoopAllocator;

    impl Allocator for NoopAllocator {
        fn request(
            &self,
            _spec: hypha_messages::WorkerSpec,
            _price: PriceRange,
            _deadline: Option<std::time::Duration>,
            _num: usize,
        ) -> impl std::future::Future<Output = Result<Vec<Worker>, AllocatorError>> + Send {
            async { Ok(Vec::new()) }
        }
    }

    #[tokio::test]
    async fn train_idle_executes_batch() {
        let pool = Pool::new(
            NoopAllocator,
            PoolConfig {
                name: "test".into(),
                spec: hypha_messages::WorkerSpec {
                    resources: Resources::default(),
                    executor: vec![],
                },
                price: PriceRange::default(),
                min: 0,
                target: 0,
                grace: Duration::from_secs(1),
            },
        );
        let worker_pool = PoolWithStatistics::<RunningMean>::new(pool);
        let worker_handle = worker_pool.handle();
        let ps_pool = Pool::new(
            NoopAllocator,
            PoolConfig {
                name: "ps".into(),
                spec: hypha_messages::WorkerSpec {
                    resources: Resources::default(),
                    executor: vec![],
                },
                price: PriceRange::default(),
                min: 0,
                target: 0,
                grace: Duration::from_secs(1),
            },
        );
        let parameter_pool = ps_pool.handle();

        let (tx, _rx) = tokio::sync::mpsc::channel::<(PeerId, Metrics)>(1);
        let resp = schedule::<RunningMean, BasicSimulation>(
            tx,
            worker_handle,
            parameter_pool,
            std::time::Instant::now(),
            (
                PeerId::random(),
                ActionRequest {
                    job_id: Uuid::new_v4(),
                    status: ExecutorStatus::Train(TrainStatus::Idle),
                },
            ),
        )
        .await
        .unwrap();

        match resp.next {
            hypha_messages::action::ExecutorAction::Train(TrainAction::ExecuteBatch) => {}
            other => panic!("Unexpected response: {:?}", other),
        }
    }
}
