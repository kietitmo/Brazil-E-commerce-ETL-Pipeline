import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from datetime import datetime

dag = DAG(
    dag_id = "ecommerce-etl",
    default_args = {
        "owner": "Kiet Nguyen",
        "start_date": days_ago(1),
        # "on_failure_callback":
    },
    schedule_interval = "59 23 * * *"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

etl_job = SparkSubmitOperator(
    task_id="etl_job",
    conn_id="spark-conn",
    application="jobs/emcommerce-etl.jar",
    name='ecommerce_etl',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.submit.deployMode': 'client',
        'spark.hadoop.fs.defaultFS': 'hdfs://namenode:8020',
        'spark.sql.warehouse.dir':'hdfs://namenode:8020/user/hive/warehouse',
        'hive.metastore.uris': 'thrift://hive-metastore:9083' 
    },
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> etl_job >> end