

class BaseDAG:

    def globalize(self) -> None:
        globals()[self.dag.dag_id] = self.dag
