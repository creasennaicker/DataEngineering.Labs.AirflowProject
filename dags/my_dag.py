from airflow import DAG

from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


def _training_model():
    return randint(1, 10)


with DAG("my_dag", start_date=datetime(2021, 12, 1),
         schedule_interval="@daily", catchup=False) as dag:
    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )

    training_model_B = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )

    training_model_C = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )



