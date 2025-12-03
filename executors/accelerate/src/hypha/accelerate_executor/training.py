import argparse
import json
import os
import time
from typing import Optional

import torch
import torch.utils.data
from accelerate import Accelerator
from safetensors.torch import save_file, save_model

from .api import Session
from .dataset import IterableStreamDataSet, dataset_wrapper
from .model import get_model
from .utils import (
    extract_gradients,
    fetch_data,
    get_adam,
    get_preprocessor,
    get_scheduler,
    merge_models,
    prepare_files,
)

FETCH_PATH = "artifacts"


def main(socket_path: str, work_dir: str, job_json: str) -> None:  # noqa: PLR0915, PLR0912
    # Background receiver context that fills a queue with update pointers
    with Session(socket_path) as session:
        job_spec = json.loads(job_json)

        executor = job_spec["executor"]
        assert executor["class"] == "train"
        config = executor["config"]

        print(json.dumps(executor))

        accelerator = Accelerator(project_dir=work_dir)

        prepare_files(config, session)
        local_fetch_path = f"{work_dir}/{FETCH_PATH}"
        print(os.listdir(local_fetch_path))

        model = get_model(local_fetch_path, config["model"]["task"])
        optimizer = get_adam(config["optimizer"], model.parameters())
        scheduler = get_scheduler(config.get("scheduler"), optimizer)
        preprocessor_config = config.get("preprocessor")
        data_loader = torch.utils.data.DataLoader(
            IterableStreamDataSet(
                fetch_data(session, config["data"], work_dir),
                config["model"]["input-names"],
                preprocessor_config["input-names"] if preprocessor_config else [],
                get_preprocessor(preprocessor_config, local_fetch_path),
            ),
            batch_size=config["batch_size"],
        )

        # Serialize the model to disk
        previous_model_path = os.path.join(work_dir, "0_global_weights.pt")
        save_model(model, previous_model_path)

        model, optimizer, training_dataloader, scheduler = accelerator.prepare(model, optimizer, data_loader, scheduler)
        training_data_iter = dataset_wrapper(training_dataloader)

        epoch_counter = 1
        job_id = job_spec["job_id"]
        MIN_LOOP_TIME_MS = 100
        last_gradient: Optional[str] = None
        last_metrics: dict[str, float] = {}

        with session.receive(config["results"], "incoming") as receiver:
            updates_iter = iter(receiver)

            current_status = {
                "executor": "train",
                "details": {"state": "idle"},
            }

            while True:
                loop_start_ms = time.time() * 1000.0
                action_resp = session.send_action({"job_id": job_id, "status": current_status})
                next_action = action_resp.get("next", {})

                if next_action.get("executor") != "train":
                    raise RuntimeError(f"Unexpected executor action: {next_action}")

                action = next_action.get("action", {})
                kind = action.get("kind")

                if kind == "terminate":
                    print("Training finished", flush=True)
                    break

                if kind == "idle":
                    timeout = action.get("timeout")
                    if timeout is not None:
                        now_ms = time.time() * 1000.0
                        delta_ms = max(0, int(timeout / 1_000_000) - int(now_ms))
                        if delta_ms > 0:
                            time.sleep(delta_ms / 1000.0)
                    current_status = {"executor": "train", "details": {"state": "idle"}}
                elif kind == "execute-batch":
                    batch = next(training_data_iter)
                    optimizer.zero_grad()
                    outputs = model(**batch)
                    loss = outputs if isinstance(outputs, torch.Tensor) else outputs["loss"]
                    accelerator.backward(loss)
                    optimizer.step()
                    scheduler.step()
                    if accelerator.is_main_process:
                        batch_size = next(iter(batch.values())).shape[0]
                        current_status = {
                            "executor": "train",
                            "details": {"state": "batch-completed", "batch_size": batch_size},
                        }
                        # Prepare gradients for potential SendUpdate
                        file_name = f"{epoch_counter}_local_gradients.pt"
                        result_path = os.path.join(work_dir, file_name)
                        model_cpu = accelerator.unwrap_model(model)
                        model_cpu.to("cpu")
                        save_file(extract_gradients(model_cpu.state_dict(), previous_model_path), result_path)
                        last_gradient = file_name
                        last_metrics = {"loss": float(loss.detach().cpu().numpy())}
                    else:
                        current_status = {
                            "executor": "train",
                            "details": {"state": "batch-completed", "batch_size": 0},
                        }
                elif kind == "send-update":
                    if last_gradient is None:
                        raise RuntimeError("SendUpdate requested but no gradients available")
                    session.send_resource(config["updates"], last_gradient)
                    current_status = {"executor": "train", "details": {"state": "sent-update"}}
                elif kind == "apply-update":
                    try:
                        pointers = next(updates_iter)
                        if pointers:
                            latest = pointers[-1] if isinstance(pointers, list) else pointers
                            parameters = (
                                latest.get("parameters") if isinstance(latest.get("parameters"), dict) else None
                            )
                            rel_path = parameters.get("path") if parameters else latest.get("path")
                            if isinstance(rel_path, str):
                                path = os.path.join(work_dir, rel_path)
                                model.load_state_dict(merge_models(previous_model_path, path))
                                save_model(model, previous_model_path)
                                model = accelerator.prepare(model)
                    except StopIteration:
                        print("Receiver stream closed; no updates to merge.")

                    current_status = {
                        "executor": "train",
                        "details": {"state": "applied-update", "round": epoch_counter, "metrics": last_metrics},
                    }
                    epoch_counter += 1
                else:
                    raise RuntimeError(f"Unhandled action kind: {kind}")

                elapsed = time.time() * 1000.0 - loop_start_ms
                if elapsed < MIN_LOOP_TIME_MS:
                    time.sleep((MIN_LOOP_TIME_MS - elapsed) / 1000.0)

        print(f"Finished training of {epoch_counter - 1} DiLoCo update rounds", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--job", required=True)
    args = parser.parse_args()
    main(args.socket, args.work_dir, args.job)
