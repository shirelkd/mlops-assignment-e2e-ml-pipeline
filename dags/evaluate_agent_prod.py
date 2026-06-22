import os
import json
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Inside the Airflow container, the project root is mapped to /sources or /opt/airflow
# But the Docker daemon runs on the host, so we need the host path for mounts.
# We assume the host path is ~/mlops-assignment-e2e-ml-pipeline
HOST_PROJECT_ROOT = "/home/shirelk/mlops-assignment-e2e-ml-pipeline"
HOST_MINI_SWE_AGENT = "/home/shirelk/mini-swe-agent"

@dag(
    dag_id="evaluate_agent_production",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string"),
        "subset": Param("verified", type="string"),
        "workers": Param(5, type="integer"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string"),
        "task_slice": Param("0:3", type="string"),
    }
)
def evaluate_agent_prod_dag():
    @task
    def prepare_run(**context):
        params = context["params"]
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        
        # Inside the Airflow container, we access it via /opt/airflow/runs
        internal_run_dir = Path("/opt/airflow/runs") / run_id
        internal_run_dir.mkdir(parents=True, exist_ok=True)
        (internal_run_dir / "run-eval").mkdir(parents=True, exist_ok=True)
        
        config = {
            "split": params["split"],
            "subset": params["subset"],
            "workers": params["workers"],
            "model": params["model"],
            "task_slice": params["task_slice"],
        }
        
        with open(internal_run_dir / "config.json", "w") as f:
            json.dump(config, f, indent=2)
            
        host_run_dir = f"{HOST_PROJECT_ROOT}/runs/{run_id}"
        
        return {
            "run_id": run_id,
            "internal_run_dir": str(internal_run_dir),
            "host_run_dir": host_run_dir
        }

    @task
    def build_agent_args(run_info, **context):
        params = context["params"]
        
        args = [
            "uv", "run", "mini-extra", "swebench",
            "--subset", params["subset"],
            "--split", params["split"],
            "--model", params["model"],
            "--config", "/mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml",
            "--workers", str(params["workers"]),
            "-o", f"/runs/{run_info['run_id']}/run-agent"
        ]
        if params.get("task_slice"):
            args.extend(["--slice", params["task_slice"]])
            
        return " ".join(args)

    @task
    def build_eval_args(run_info, **context):
        params = context["params"]
        
        run_dir = f"/runs/{run_info['run_id']}/run-eval"
        
        cmd = (
            f"bash -c 'cd /mlops-assignment && /mlops-assignment/.venv/bin/python -m swebench.harness.run_evaluation "
            f"--dataset_name princeton-nlp/SWE-bench_Verified "
            f"--predictions_path /runs/{run_info['run_id']}/run-agent/preds.json "
            f"--max_workers {params['workers']} "
            f"--run_id {run_info['run_id']} && "
            f"mv *.json {run_dir}/ || true && mv logs {run_dir}/ || true'"
        )
        return cmd

    run_info = prepare_run()
    
    agent_cmd = build_agent_args(run_info)
    
    run_agent = DockerOperator(
        task_id="run_agent",
        image="swe-agent-worker:latest",
        api_version="auto",
        auto_remove="force",
        command="{{ task_instance.xcom_pull(task_ids='build_agent_args') }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=f"{HOST_PROJECT_ROOT}/runs", target="/runs", type="bind"),
            Mount(source=HOST_MINI_SWE_AGENT, target="/mini-swe-agent", type="bind", read_only=True),
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
        ],
        environment={
            "MSWEA_COST_TRACKING": "ignore_errors",
            "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
        }
    )
    
    eval_cmd = build_eval_args(run_info)
    
    run_eval = DockerOperator(
        task_id="run_eval",
        image="swe-agent-worker:latest",
        api_version="auto",
        auto_remove="force",
        command="{{ task_instance.xcom_pull(task_ids='build_eval_args') }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=f"{HOST_PROJECT_ROOT}/runs", target="/runs", type="bind"),
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
        ],
    )
    
    @task
    def summarize_and_log(run_info, **context):
        import json
        import mlflow
        
        internal_run_dir = Path(run_info["internal_run_dir"])
        eval_dir = internal_run_dir / "run-eval"
        
        metrics = {
            "status": "completed",
            "eval_finished": True,
            "resolved_instances": 0,
            "total_instances": 0,
            "success_rate": 0.0
        }
        
        # Find the report JSON
        report_files = list(eval_dir.glob("*.json"))
        if report_files:
            try:
                with open(report_files[0], "r") as f:
                    report = json.load(f)
                    metrics["total_instances"] = report.get("total_instances", 0)
                    metrics["resolved_instances"] = report.get("resolved_instances", 0)
                    if metrics["total_instances"] > 0:
                        metrics["success_rate"] = metrics["resolved_instances"] / metrics["total_instances"]
            except Exception as e:
                print(f"Failed to parse report: {e}")
        
        with open(internal_run_dir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)
            
        manifest = {
            "run_id": run_info["run_id"],
            "artifacts_dir": str(internal_run_dir)
        }
        with open(internal_run_dir / "manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)
            
        # Log to MLflow
        # The MLflow server runs on mlflow:5000 inside docker-compose
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.set_experiment("evaluate_agent_s3_production")
        
        with mlflow.start_run(run_name=run_info["run_id"]):
            params = context["params"]
            mlflow.log_params(params)
            mlflow_metrics = {k: v for k, v in metrics.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}
            mlflow.log_metrics(mlflow_metrics)
            
            # Log the artifact URI as a tag for easy viewing in the UI (like the previous version)
            mlflow.set_tag("artifact_uri", str(internal_run_dir))
            
            # Since we deployed MinIO and configured MLflow's default artifact root to s3://mlflow-artifacts,
            # this will correctly push the entire artifact folder securely into our object storage!
            mlflow.log_artifacts(str(internal_run_dir))
            
        return run_info

    run_info >> agent_cmd >> run_agent >> eval_cmd >> run_eval >> summarize_and_log(run_info)

evaluate_agent_prod_dag()
