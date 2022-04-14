from airflow_dbt.hooks.dbt_hook import DbtCliHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow_dbt.operators.dbt_operator import DbtBaseOperator


class DbtSeedOperator(DbtBaseOperator):
    # note: without forking the hook code, we don't currently have a way to pass the --select operation
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super(DbtSeedOperator, self).__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)

    def execute(self, context):
        self.create_hook().run_cli('seed')


class DbtRunOperationOperator(DbtBaseOperator):
    # note: without forking the hook code, we don't currently have a way to pass the --args flag to run-operation
    @apply_defaults
    def __init__(self, op_name, profiles_dir=None, target=None, *args, **kwargs):
        super(DbtRunOperationOperator, self).__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)
        self.op_name = op_name

    def execute(self, context):
        # note: again, by making our own package, we could pass the operation name in a better way
        self.create_hook().run_cli('run-operation', self.op_name)