"""DAGs for the nightly contract test: one of each dependency kind airflow-depgraph reads.

producer --asset--> consumer
trigger --TriggerDagRunOperator--> triggered
waiter --ExternalTaskSensor / PythonSensor (op_kwargs external_dag_id)--> producer
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, Asset, task

ORDERS = Asset("contract://orders")
START = datetime(2026, 1, 1)
COMMON = {"start_date": START, "catchup": False, "tags": ["contract"]}


def _producer_ready(external_dag_id: str) -> bool:
    return bool(external_dag_id)


with DAG("contract_producer", schedule=None, **COMMON):

    @task(outlets=[ORDERS])
    def produce() -> str:
        return "orders"

    produce()

with DAG("contract_consumer", schedule=[ORDERS], **COMMON):
    EmptyOperator(task_id="consume")

with DAG("contract_trigger", schedule=None, **COMMON):
    TriggerDagRunOperator(
        task_id="trigger_triggered",
        trigger_dag_id="contract_triggered",
        wait_for_completion=False,
    )

with DAG("contract_triggered", schedule=None, **COMMON):
    EmptyOperator(task_id="work")

with DAG("contract_waiter", schedule=None, **COMMON):
    ExternalTaskSensor(
        task_id="wait_external",
        external_dag_id="contract_producer",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=5,
        timeout=60,
        soft_fail=True,
    )
    PythonSensor(
        task_id="wait_python",
        python_callable=_producer_ready,
        op_kwargs={"external_dag_id": "contract_producer"},
        poke_interval=5,
        timeout=60,
    )
