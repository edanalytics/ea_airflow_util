

class BaseDAG:

    def globalize(self):
        globals()[self.dag.dag_id] = self.dag
