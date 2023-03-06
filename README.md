## dag.test() example repository

This repository contains the code shown in the LIVE with Astronomer on 2023-03-07, showing how to use `dag.test()`.

### How to use this repository

1. Clone the repository.
2. Make sure you are in an environment that has Airflow 2.5+ installed by running `airflow version` from your command line.
3. Make sure you are in an environment that has the [Airflow Amazon provider](https://registry.astronomer.io/providers/amazon) installed. If you have virtualenv installed (`virtualenv --version`) then you can enter the venv provided in this repo by running `source venv/bin/activate`.
4. Provide your own Amazon connection by creating a `connections.yaml` file based upon `connections_template.yaml`. 
5. Create an S3 bucket `live-dag-test-bucket` containing one JSON file named `order_values.json` containing the following JSON string: `{'1001': 301.27, '1002': 433.21, '1003': 502.22}`.
6. Run `python dag_test_example.py`.

### Relevant resources

- [Concept guide: Test Airflow DAGs](https://docs.astronomer.io/learn/testing-airflow)
- Airflow Documentation: [Testing DAGs with dag.test()](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/debug.html)
- Python Documentation: [The Python Debugger](https://docs.python.org/3/library/pdb.html)
- [How to install Airflow locally](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- Concept guide: [Custom Hooks and Operators](https://docs.astronomer.io/learn/airflow-importing-custom-hooks-operators)
- [Source code dag object](https://github.com/apache/airflow/blob/main/airflow/models/dag.py)
- [Debugging in VSCode](https://code.visualstudio.com/docs/editor/debugging)
