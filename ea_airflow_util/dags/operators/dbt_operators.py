### NOTE! This module will be imminently deprecated and replaced with `ea_airflow_util.providers.dbt.operators.dbt`.
# This version of the module CANNOT handle schema swaps.


# AirflowDBT uses Airflow 1.x syntax when defining Hooks and Operators.
# These warnings clog up the scheduler and should be hidden until the package is updated.
import warnings
warnings.filterwarnings("ignore", module="airflow_dbt", category=DeprecationWarning)

from airflow.utils.decorators import apply_defaults
from airflow_dbt.operators.dbt_operator import DbtBaseOperator


class DbtRunOperationOperator(DbtBaseOperator):
    # note: without forking the hook code, we don't currently have a way to pass the --args flag to run-operation
    @apply_defaults
    def __init__(self, op_name, profiles_dir=None, target=None, *args, **kwargs):
        super(DbtRunOperationOperator, self).__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)
        self.op_name = op_name

    def execute(self, context):
        # note: again, by making our own package, we could pass the operation name in a better way
        self.create_hook().run_cli('run-operation', self.op_name)