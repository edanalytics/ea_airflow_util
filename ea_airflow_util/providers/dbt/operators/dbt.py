# AirflowDBT uses Airflow 1.x syntax when defining Hooks and Operators.
# These warnings clog up the scheduler and should be hidden until the package is updated.
import warnings
warnings.filterwarnings("ignore", module="airflow_dbt", category=DeprecationWarning)

import json

from typing import Optional

from airflow_dbt.operators.dbt_operator import DbtBaseOperator


class DbtRunOperationOperator(DbtBaseOperator):
    """
    Without forking the hook code, we don't have a way to pass the --args flag to run-operation.
    """
    def __init__(self, op_name, arguments: Optional[dict] = None, *args, **kwargs):
        super(DbtRunOperationOperator, self).__init__(*args, **kwargs)
        self.op_name = op_name
        self.arguments = arguments

    def execute(self, **context):
        """
        dbt run-operation [OPTIONS] MACRO
        """
        cmd_pieces = ['run-operation', self.op_name]

        if self.arguments:
            cmd_pieces.extend(["--args", f'{json.dumps(self.arguments)}'])

        self.create_hook().run_cli(*cmd_pieces)
