from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# El comando de impersonation se antepone a cada comando de dbt,
# para que ambos actuen con la identidad de airflow-runner, no la tuya.
IMPERSONATE = (
    "gcloud auth application-default login "
    "--impersonate-service-account=airflow-runner@pedidos-dataeng.iam.gserviceaccount.com "
    "--quiet && "
)

RUTA_PROYECTO_DBT = "/home/stivenjc/PycharmProjects/proyecto_dbt_bigquery/proyecto_pedidos"
ACTIVAR_VENV = "source /home/stivenjc/PycharmProjects/proyecto_dbt_bigquery/venv_dbt/bin/activate && "

with DAG(
    dag_id="pedidos_dbt_bigquery",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{ACTIVAR_VENV} cd {RUTA_PROYECTO_DBT} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{ACTIVAR_VENV} cd {RUTA_PROYECTO_DBT} && dbt test",
    )

    dbt_run >> dbt_test