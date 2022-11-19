from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

# ===============================================================================

# Criando parametro de data para rodar a dag
ontem = datetime.today() - timedelta(days=1)

# formato: YYYYM
ano = ontem.strftime("%Y")
mes = str(int(ontem.strftime("%m")))
data_parametro = ano + mes

# ===============================================================================

# Definindo DAG

dag = DAG(
    dag_id="prova",
    start_date=datetime(2021, 1, 1),
    # rodar uma ver por dia a 00:30
    schedule_interval="30 0 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "airflow",
        "depends_on_past": False
    }
)

# ===============================================================================
bronze = "/bronze/shell/"
silver = "/silver/shell/"

# ===============================================================================

task_vra_silver = BashOperator(
    task_id="vra_silver",
    bash_command=f"spark-submit {silver}vra_silver.sh",
    # Pass the parameter date to the script
    env={"date_parameter": data_parametro},
    dag=dag
)


task_air_cia_silver = BashOperator(
    task_id="air_cia_silver",
    bash_command=f"spark-submit {silver}air_cia_silver.sh",
    # Pass the parameter date to the script
    env={"date_parameter": data_parametro},
    dag=dag
)

task_aerodromos_bronze = BashOperator(
    task_id="aerodromos_bronze",
    bash_command=f"spark-submit {bronze}aerodromos_bronze.sh",
    # Pass the parameter date to the script
    env={"date_parameter": data_parametro},
    dag=dag
)

# ===============================================================================

task_vra_silver >> task_air_cia_silver >> task_aerodromos_bronze
