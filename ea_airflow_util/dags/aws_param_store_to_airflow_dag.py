import logging

import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection

from .dag_util.ssm_parameter_store import SSMParameterStore


class AWSParamStoreToAirflowDAG():
    """

    """
    def __init__(self, ssm_prefix, s3_region, **kwargs):
        self.ssm_prefix = ssm_prefix
        self.s3_region = s3_region
        self.dag = self.build_dag(**kwargs)


    def build_dag(self, dag_id, default_args, **kwargs):
        """

        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :param catchup:
        :return:
        """

        @task
        def insert_all_aws_params_to_airflow():
            """
            :return:
            """
            param_store = SSMParameterStore(prefix=self.ssm_prefix, region_name=self.s3_region)

            for param_name in param_store.keys():
                param_secret = param_store[param_name]
                # TODO: Strip off ssm_prefix from ParamStore path.
                self.create_conn(**param_secret)


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
    def create_conn(conn_id, conn_type, host, schema, login, password, port, extra):
        """
        Store a new connection in Airflow Meta DB
        TODO: Consider using **kwargs to make this more flexible, if possible.

        :param conn_id:
        :param conn_type:
        :param host:
        :param schema:
        :param login:
        :param password:
        :param port:
        :param extra:
        :return:
        """
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            schema=schema,
            login=login,
            password=password,
            port=port,
            extra=extra
        )

        session = airflow.settings.Session()
        conn_name = (
            session
                .query(Connection)
                .filter(Connection.conn_id == conn.conn_id)
                .first()
        )

        if str(conn_name) == str(conn.conn_id):
            logging.warning(
                f"Connection {conn.conn_id} already exists!"
            )
            return None

        session.add(conn)
        session.commit()

        logging.info(Connection.log_info(conn))
        logging.info(
            f"Connection {conn_id} was added."
        )

        return conn


    def globalize(self):
        globals()[self.dag.dag_id] = self.dag
