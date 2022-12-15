import json
import logging

import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection

from .dag_util.ssm_parameter_store import SSMParameterStore


class AWSParamStoreToAirflowDAG:
    """

    """
    def __init__(self,
        ssm_prefix: str,
        region_name: str,
        **kwargs
    ):
        self.ssm_prefix = ssm_prefix
        self.region_name = region_name
        self.dag = self.build_dag(**kwargs)


    def build_dag(self,
        dag_id: str,
        default_args: dict,
        **kwargs
    ):
        """

        :param dag_id:
        :param default_args:
        :return:
        """

        @task
        def insert_all_aws_params_to_airflow():
            """
            :return:
            """
            param_store = SSMParameterStore(prefix=self.ssm_prefix, region_name=self.region_name)

            for param_name in param_store.keys():
                conn_id = param_name.replace(self.ssm_prefix, "")
                param_secret = json.loads(param_store[param_name])
                self.create_conn(conn_id=conn_id, **param_secret)


        # This syntax ensures param_store stays hidden within the class.
        with DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule_interval=None,
            catchup=False,
        ) as dag:
            insert_all_aws_params_to_airflow()

        return dag


    # stackoverflow link:
    # https://stackoverflow.com/questions/51863881/is-there-a-way-to-create-modify-connections-through-airflow-api
    @staticmethod
    def create_conn(conn_id: str, **kwargs) -> Connection:
        """
        Store a new connection in Airflow Meta DB

        :param conn_id:
        :param kwargs:
        :return:

        :Keyword Arguments:
            * conn_type
            * host
            * schema
            * login
            * password
            * port
            * extra
        """
        conn = Connection(conn_id=conn_id, **kwargs)
        session = airflow.settings.Session()

        # Verify whether the connection already exists in Airflow.
        if session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
            logging.warning(
                f"Connection {conn.conn_id} already exists!"
            )
            return None

        else:
            session.add(conn)
            session.commit()

            logging.info(Connection.log_info(conn))
            logging.info(
                f"Connection {conn_id} was added."
            )

            return conn
