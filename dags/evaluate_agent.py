import os
import json
import shutil
from datetime import datetime
from pathlib import Path
import subprocess

from airflow.decorators import dag, task
from airflow.models.param import Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"

@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string"),
        "subset": Param("verified", type="string"),
        "workers": Param(5, type="integer"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string"),
        "task_slice": Param("0:3", type="string"),
        "cost_limit": Param("0", type="string"),
    }
)
def evaluate_agent_dag():
    @task
    def prepare_run(**context):
        params = context["params"]
        # Use airflow's run_id which is guaranteed to be unique
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        
        config = {
            "split": params["split"],
            "subset": params["subset"],
            "workers": params["workers"],
            "model": params["model"],
            "task_slice": params["task_slice"],
            "cost_limit": params["cost_limit"],
        }
        
        with open(run_dir / "config.json", "w") as f:
            json.dump(config, f, indent=2)
            
        return {"run_dir": str(run_dir), "run_id": run_id}

    @task
    def run_agent(run_info, **context):
        params = context["params"]
        run_dir = Path(run_info["run_dir"])
        agent_dir = run_dir / "run-agent"
        agent_dir.mkdir(parents=True, exist_ok=True)
        
        args = [
            "uv", "run", "mini-extra", "swebench",
            "--subset", params["subset"],
            "--split", params["split"],
            "--model", params["model"],
            "--config", str(PROJECT_ROOT.parent / "mini-swe-agent" / "src" / "minisweagent" / "config" / "benchmarks" / "swebench.yaml"),
            "--workers", str(params["workers"]),
            "-o", str(agent_dir)
        ]
        
        if params.get("task_slice"):
            args.extend(["--slice", params["task_slice"]])
            
        # Add tracking environment
        env = os.environ.copy()
        env["MSWEA_COST_TRACKING"] = "ignore_errors"
        
        subprocess.run(args, cwd=PROJECT_ROOT, env=env, check=True)
        return run_info

    @task
    def run_eval(run_info, **context):
        params = context["params"]
        run_dir = Path(run_info["run_dir"])
        agent_dir = run_dir / "run-agent"
        eval_dir = run_dir / "run-eval"
        eval_dir.mkdir(parents=True, exist_ok=True)
        
        preds_path = agent_dir / "preds.json"
        
        args = [
            "uv", "run", "python", "-m", "swebench.harness.run_evaluation",
            "--dataset_name", "princeton-nlp/SWE-bench_Verified",
            "--predictions_path", str(preds_path),
            "--max_workers", str(params["workers"]),
            "--run_id", run_info["run_id"]
        ]
        
        subprocess.run(args, cwd=PROJECT_ROOT, check=True)
        return run_info

    @task
    def summarize_and_log(run_info, **context):
        run_dir = Path(run_info["run_dir"])
        
        # SWE-bench typically saves a report or log files. We just do basic manifest and logging
        metrics = {
            "status": "completed",
            "eval_finished": True
        }
        
        with open(run_dir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)
            
        manifest = {
            "run_id": run_info["run_id"],
            "artifacts_dir": str(run_dir),
            "remote_storage": "To upload this run, use: aws s3 sync runs/<run-id> s3://my-bucket/runs/<run-id>"
        }
        with open(run_dir / "manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)
            
        # If mlflow was used, we would log here:
        # import mlflow
        # mlflow.log_params(...)
        # mlflow.log_metrics(...)
        # mlflow.log_artifacts(str(run_dir))
        
        return run_info

    run_info = prepare_run()
    agent_info = run_agent(run_info)
    eval_info = run_eval(agent_info)
    summarize_and_log(eval_info)

evaluate_agent_dag()
