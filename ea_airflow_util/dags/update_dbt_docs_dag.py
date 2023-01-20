import os
from util import io_helpers
from typing import Optional

from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtDocsGenerateOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# You can pass the S3 `bucket`` to this function, but if not, it will use the bucket defined in the Schema from
# the S3 Airflow connection.
# (see https://stackoverflow.com/questions/72091014/how-do-i-specify-a-bucket-name-using-an-s3-connection-in-airflow)
def upload_to_s3(conn_id: str, filename: str, key: str) -> None: # , bucket_name: str
    content_type = "application/json"
    if ".html" in filename: content_type = "text/html"
    if ".css" in filename: content_type = "text/css"
    if ".svg" in filename: content_type = "image/svg+xml"
    hook = S3Hook(conn_id, extra_args={"ContentType":content_type})
    hook.load_file(filename=filename, key=key, replace=True) # , bucket_name=bucket_name)


class UpdateDbtDocsDag():
    """
    params: environment 
    params: dbt_repo_path 
    params: dbt_target_name 
    params: dbt_bin_path 
    
    """
    def __init__(self,
        # environment: str,
    
        # required dbt paths and target
        dbt_repo_path  : str,
        dbt_target_name: str,
        dbt_bin_path   : str,
        dbt_docs_s3_conn_id : str,
        dbt_docs_custom_html: Optional[str] = None,
        dbt_docs_custom_css: Optional[str] = None,
        dbt_docs_images: Optional[list] = None,

        **kwargs
    ):
        # self.environment = environment
        self.dbt_docs_s3_conn_id = dbt_docs_s3_conn_id
        
        # dbt paths
        self.dbt_repo_path = dbt_repo_path
        self.dbt_target_name = dbt_target_name
        self.dbt_bin_path = dbt_bin_path
        self.dbt_docs_custom_html = dbt_docs_custom_html
        self.dbt_docs_custom_css = dbt_docs_custom_css
        self.dbt_docs_images = dbt_docs_images


        self.dag = self.initialize_dag(**kwargs)


    # create DAG 
    def initialize_dag(self, dag_id, schedule_interval, default_args, **kwargs):
        """
        :param dag_id:
        :param schedule_interval:
        :param default_args:
        :param catchup:
        :user_defined_macros:
        """
        return DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            catchup=False,
            # user_defined_macros= {
            #     'environment': self.environment,
            # }
        )

    def update_dbt_docs(self, on_success_callback=None, **kwargs):

        dbt_docs_generate_task = DbtDocsGenerateOperator(
                task_id= f'dbt_generate_docs',
                dir    = self.dbt_repo_path,
                target = self.dbt_target_name,
                dbt_bin= self.dbt_bin_path,
                dag=self.dag
            )
        
        docs_files = ["target/index.html", "target/catalog.json", "target/manifest.json"]
        # if a custom html file exists, replace the file path with configured path. do the same for css if exists
        if self.dbt_docs_custom_html:
          docs_files.remove("target/index.html")
          docs_files.append(self.dbt_docs_custom_html)
        if self.dbt_docs_custom_css:
          docs_files.append(self.dbt_docs_custom_css)
        if self.dbt_docs_images:
            for img in self.dbt_docs_images:
                docs_files.append(img)
            
        upload_tasks = []
        for docs_file in docs_files:
            # e.g. docs_file = 'target/index.html" -> s3_key = "index.html" -> task_id = "index"
            s3_key = docs_file.split("/")[-1]
            task_id = s3_key.split(".")[0]

            upload_tasks.append(
                PythonOperator(
                    task_id='upload_to_s3_' + task_id,
                    python_callable=upload_to_s3,
                    op_kwargs={
                        'conn_id': self.dbt_docs_s3_conn_id,
                        'filename': os.path.join(self.dbt_repo_path, docs_file),
                        'key': s3_key
                    },
                    dag=self.dag
                )
            )

        dbt_docs_generate_task >> upload_tasks
