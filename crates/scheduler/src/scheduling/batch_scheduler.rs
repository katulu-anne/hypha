use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime, Instant},
};

use hypha_messages::{
    Reference, SelectionStrategy,
    action::{
        self, AggregateAction, AggregateError, AggregateStatus, ExecutorAction, ExecutorStatus,
        TrainAction, TrainError, TrainStatus,
    },
    progress::Metrics,
};
use hypha_network::request_response::{RequestResponseError, RequestResponseInterfaceExt};
use libp2p::PeerId;
use thiserror::Error;
use tokio::{
    sync::{Mutex, mpsc::{self, Sender, error::SendError}},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    network::Network,
    pool::{PoolHandle, PoolStatisticsHandle},
    simulation::Simulation,
    statistics::RuntimeStatistic,
};

// NOTE: Tracks per-round update signals from workers so the scheduler can
// decide when to instruct the parameter server to aggregate.
#[derive(Default)]
struct RoundState {
    sent_updates: HashSet<PeerId>,
    first_update_at: Option<Instant>,
    min_quorum: usize,
    grace: Duration,
    round: u32,
}

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
    round_state: Arc<Mutex<RoundState>>,
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

    let since_start = start.elapsed().as_millis() as u64;
    // NOTE: We rely on Pool::members() being oldest-first ordered by join time.
    let parameter_servers: Vec<PeerId> =
        parameter_pool.members().iter().map(|w| w.peer_id).collect();
    let primary_ps = parameter_servers.first().copied();

    let response = match status {
        ExecutorStatus::Train(train) => match train {
            TrainStatus::Idle => ExecutorAction::Train(TrainAction::ExecuteBatch),
            TrainStatus::BatchCompleted { .. } => {
                worker_pool.update(&peer_id, since_start);
                if parameter_servers.is_empty() {
                    return Ok(action::ActionResponse {
                        job_id,
                        next: ExecutorAction::Train(TrainAction::Idle {
                            timeout: now + Duration::from_secs(1),
                        }),
                    });
                }

                ExecutorAction::Train(TrainAction::SendUpdate {
                    target: Reference::Peers {
                        // Selecting a single PS to avoid that workers send updates to multiple PS
                        peers: vec![parameter_servers[0]],
                        strategy: SelectionStrategy::One,
                        resource: None,
                    },
                    // TODO: We need a way to properly determine a good sent timeout
                    timeout: now + Duration::from_secs(30),
                })
            }
            TrainStatus::SentUpdate => {
                // NOTE: Track workers that have sent their update for the current round.
                let mut state = round_state.lock().await;
                state.sent_updates.insert(peer_id);
                if state.first_update_at.is_none() {
                    state.first_update_at = Some(Instant::now());
                }
                let total_workers = worker_pool.statistics().len();
                let sent = state.sent_updates.len();
                let elapsed_ms = state
                    .first_update_at
                    .map(|t| t.elapsed().as_millis() as u64)
                    .unwrap_or(0);
                tracing::info!(
                    %peer_id,
                    round = state.round,
                    sent,
                    total = total_workers,
                    min_quorum = state.min_quorum,
                    grace_ms = state.grace.as_millis() as u64,
                    since_first_ms = elapsed_ms,
                    "Worker reported SentUpdate; recorded for round"
                );
                if parameter_servers.is_empty() {
                    ExecutorAction::Train(TrainAction::Idle {
                        timeout: now + Duration::from_secs(1),
                    })
                } else {
                    ExecutorAction::Train(TrainAction::ApplyUpdate {
                        source: Reference::Peers {
                            peers: parameter_servers,
                            strategy: SelectionStrategy::All,
                            resource: None,
                        },
                        timeout: now + Duration::from_secs(30),
                    })
                }
            }
            TrainStatus::AppliedUpdate { round, metrics } => {
                tx.send((peer_id, Metrics { round, metrics }))
                    .await
                    .map_err(BatchSchedulerError::from)?;

                ExecutorAction::Train(TrainAction::ExecuteBatch)
            }
            TrainStatus::Error(TrainError::Connection { message }) => {
                tracing::warn!(%peer_id, message = %message, "Worker reported connection error");
                ExecutorAction::Train(TrainAction::Idle {
                    timeout: now + Duration::from_secs(1),
                })
            }
            TrainStatus::Error(TrainError::Other { message }) => {
                tracing::warn!(%peer_id, message = %message, "Worker reported error");
                ExecutorAction::Train(TrainAction::Terminate)
            }
            TrainStatus::Terminated => ExecutorAction::Train(TrainAction::Terminate),
        },
        ExecutorStatus::Aggregate(state) => match state {
            AggregateStatus::Idle => {
                // Only the primary PS is allowed to aggregate.
                if Some(peer_id) != primary_ps {
                    if let Some(primary) = primary_ps {
                        tracing::debug!(
                            %peer_id,
                            primary_ps = %primary,
                            "Non-primary PS polling; returning Idle"
                        );
                    }
                    ExecutorAction::Aggregate(AggregateAction::Idle {
                        timeout: now + Duration::from_secs(5),
                    })
                } else {
                    let workers: Vec<_> = worker_pool
                        .statistics()
                        .into_iter()
                        .map(|w| w.peer_id)
                        .collect();

                    if workers.is_empty() {
                        ExecutorAction::Aggregate(AggregateAction::Idle {
                            timeout: now + Duration::from_secs(1),
                        })
                    } else {
                        // Start aggregation when either all workers have sent updates,
                        // or when a quorum (min workers) have sent updates and the
                        // grace period has elapsed since the first update in this round.
                        let state = round_state.lock().await;
                        let all_sent = workers
                            .iter()
                            .all(|w| state.sent_updates.contains(w));
                        let effective_quorum = state.min_quorum.min(workers.len());
                        let quorum_met = state.sent_updates.len() >= effective_quorum;
                        let timebox_elapsed = state
                            .first_update_at
                            .map(|t| t.elapsed() >= state.grace)
                            .unwrap_or(false);
                        let ready = all_sent || (quorum_met && timebox_elapsed);
                        let reason = if all_sent {
                            "all_sent"
                        } else if quorum_met && timebox_elapsed {
                            "quorum_and_grace"
                        } else if quorum_met {
                            "quorum_met_waiting_grace"
                        } else {
                            "waiting_quorum"
                        };
                        tracing::info!(
                            round = state.round,
                            workers = workers.len(),
                            sent = state.sent_updates.len(),
                            min_quorum = state.min_quorum,
                            effective_quorum,
                            grace_ms = state.grace.as_millis() as u64,
                            since_first_ms = state
                                .first_update_at
                                .map(|t| t.elapsed().as_millis() as u64)
                                .unwrap_or(0),
                            ready,
                            reason,
                            "Aggregation readiness evaluation"
                        );

                        if ready {
                            tracing::info!(round = state.round, "Trigger AggregateUpdates");
                            ExecutorAction::Aggregate(AggregateAction::AggregateUpdates {
                                source: Reference::Peers {
                                    peers: workers,
                                    strategy: SelectionStrategy::All,
                                    resource: None,
                                },
                            })
                        } else {
                            ExecutorAction::Aggregate(AggregateAction::Idle {
                                timeout: now + Duration::from_millis(500),
                            })
                        }
                    }
                }
            }
            AggregateStatus::AggregatedUpdates { .. } => {
                // Only allow the primary PS to proceed to broadcast.
                if Some(peer_id) != primary_ps {
                    ExecutorAction::Aggregate(AggregateAction::Idle {
                        timeout: now + Duration::from_secs(5),
                    })
                } else {
                    let workers: Vec<_> = worker_pool
                        .statistics()
                        .into_iter()
                        .map(|w| w.peer_id)
                        .collect();

                    if workers.is_empty() {
                        ExecutorAction::Aggregate(AggregateAction::Idle {
                            timeout: now + Duration::from_secs(1),
                        })
                    } else {
                        // Log that we are moving to broadcast for this round.
                        let state = round_state.lock().await;
                        tracing::info!(round = state.round, "Trigger BroadcastUpdate");
                        ExecutorAction::Aggregate(AggregateAction::BroadcastUpdate {
                            target: Reference::Peers {
                                peers: workers,
                                strategy: SelectionStrategy::All,
                                resource: None,
                            },
                        })
                    }
                }
            }
            AggregateStatus::BroadcastedUpdate { metrics } => {
                if let Some(metrics) = metrics {
                    tx.send((peer_id, Metrics { round: 0, metrics }))
                        .await
                        .map_err(BatchSchedulerError::from)?;
                }
                // Reset round state after completing a broadcast on the primary PS.
                if Some(peer_id) == primary_ps {
                    let mut state = round_state.lock().await;
                    tracing::info!(round = state.round, "Broadcast completed; advancing round");
                    state.sent_updates.clear();
                    state.first_update_at = None;
                    state.round = state.round.saturating_add(1);
                    tracing::info!(round = state.round, "Next round started");
                }
                ExecutorAction::Aggregate(AggregateAction::Idle {
                    timeout: now + Duration::from_secs(1),
                })
            }
            AggregateStatus::Error(AggregateError::Connection { message }) => {
                tracing::warn!(%peer_id, message = %message, "Aggregator reported connection error");
                ExecutorAction::Aggregate(AggregateAction::Idle {
                    timeout: now + Duration::from_secs(1),
                })
            }
            AggregateStatus::Error(AggregateError::Other { message }) => {
                tracing::warn!(%peer_id, message = %message, "Aggregator reported error");
                ExecutorAction::Aggregate(AggregateAction::Terminate)
            }

            AggregateStatus::Terminated => ExecutorAction::Aggregate(AggregateAction::Terminate),
        },
    };

    tracing::debug!(%peer_id, %job_id, response = ?response, "Sending action response");

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
        min_quorum: usize,
        grace: Duration,
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
            // NOTE: Track per-round SentUpdate signals to decide when to trigger aggregation.
            let round_state = Arc::new(Mutex::new(RoundState {
                sent_updates: HashSet::new(),
                first_update_at: None,
                min_quorum,
                grace,
                round: 0,
            }));
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
                    let round_state = round_state.clone();
                    async move {
                        match schedule::<T, S>(
                            tx,
                            worker_pool,
                            parameter_pool,
                            round_state,
                            start,
                            request,
                        )
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

    use super::{schedule, RoundState};
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
        let round = std::sync::Arc::new(tokio::sync::Mutex::new(RoundState::default()));
        let resp = schedule::<RunningMean, BasicSimulation>(
            tx,
            worker_handle,
            parameter_pool,
            round,
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

    #[tokio::test]
    async fn train_waits_without_parameter_server() {
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
        let round = std::sync::Arc::new(tokio::sync::Mutex::new(RoundState::default()));
        let resp = schedule::<RunningMean, BasicSimulation>(
            tx,
            worker_handle,
            parameter_pool,
            round,
            std::time::Instant::now(),
            (
                PeerId::random(),
                ActionRequest {
                    job_id: Uuid::new_v4(),
                    status: ExecutorStatus::Train(TrainStatus::BatchCompleted { batch_size: 4 }),
                },
            ),
        )
        .await
        .unwrap();

        match resp.next {
            hypha_messages::action::ExecutorAction::Train(TrainAction::Idle { .. }) => {}
            other => panic!("Unexpected response: {:?}", other),
        }
    }
}
