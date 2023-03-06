import json
from airflow.models import Variable
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from custom_operator import MyBasicMathOperator

S3_BUCKET_NAME = "live-dag-test-bucket"


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    params={"discount": 1},
)
def dag_test_example():

    breakpoint()

    @task
    def extract_orders():
        hook = S3Hook(aws_conn_id="aws_account_1_conn")
        key = "order_values.json"

        filename = hook.download_file(
            key=key, bucket_name=S3_BUCKET_NAME, local_path="/tmp"
        )

        with open(filename, "r") as f:
            output = json.load(f)

        os.remove(filename)

        breakpoint()

        return output

    @task
    def sum_orders_plus_shipping(order_values):
        shipping_cost = Variable.get("shipping_cost")["value"]
        breakpoint()
        return sum(order_values.values()) + shipping_cost


    apply_discount = MyBasicMathOperator(
        task_id="apply_discount",
        first_number="{{ ti.xcom_pull(task_ids='sum_orders_plus_shipping', key='return_value')}}",
        second_number="{{ params.discount }}",
        operation="*",
    )

    sum_orders_plus_shipping(extract_orders()) >> apply_discount


dag = dag_test_example()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "connections.yaml"
    variables_path = "variables.yaml"
    my_discount = 0.9

    dag.test(
        execution_date=datetime(2023, 1, 10),
        conn_file_path=conn_path,
        variable_file_path=variables_path,
        run_conf={"discount": my_discount},
    )
