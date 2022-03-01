import logging
import os
from airflow import DAG, settings
from airflow.models import Connection

# found this open source SSM class
# copied by RL on 3/1/2022 from this github gist: https://gist.github.com/nqbao/9a9c22298a76584249501b74410b8475
from ssm_parameter_store import SSMParameterStore

class AWSParamStoreToAirflowDAG:

    def __init__(self,
                 ssm_prefix
                 ):

        self.ssm_prefix = ssm_prefix


    # stackoverflow link: https://stackoverflow.com/questions/51863881/is-there-a-way-to-create-modify-connections-through-airflow-api
    @staticmethod
    def create_conn(conn_id, conn_type, host, login, password, port, description):
        """
        Store a new connection in Airflow db
        """

        conn = Connection(conn_id=conn_id,
                          conn_type=conn_type,
                          host=host,
                          schema=schema,
                          login=login,
                          password=password,
                          port=port,
                          extra=extra)
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

        if str(conn_name) == str(conn.conn_id):
            logging.warning(f"Connection {conn.conn_id} already exists")
            return None

        session.add(conn)
        session.commit()
        logging.info(Connection.log_info(conn))
        logging.info(f'Connection {conn_id} is created')

        return conn


    # this should work after IAM role is set up on the EC2?
    @staticmethod
    def insert_all_aws_params(ssm_prefix):
        """
        Loop over aws params in a given folder, insert each one as a new connection in Airflow db
        """

        this_store = SSMParameterStore(prefix=ssm_prefix)

        for this_key in this_store.keys():

            this_secret = this_store[this_key]

            # create airflow connection using data from the secret
            create_conn(conn_id=this_secret['conn_id'],
                        conn_type=this_secret['conn_type'],
                        host=this_secret['host'],
                        schema=this_secret['schema'],
                        login=this_secret['login'],
                        password=this_secret['password'],
                        port=this_secret['port'],
                        extra=this_secret['description'],
                        )

    def initialize_dag(self,ssm_prefix=self.ssm_prefix, **kwargs):

        with DAG(**kwargs
        ) as dag:

            run_insert_all_aws_params = PythonOperator(
                task_id="insert_all_aws_params",
                python_callable=insert_all_aws_params,
                op_kwargs={'ssm_prefix': ssm_prefix},
                provide_context=True
            )

        return dag