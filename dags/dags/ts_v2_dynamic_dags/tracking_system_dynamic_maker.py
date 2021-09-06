import logging

from edo.dagger.dag_maker import DagMaker

DAG_ID = "TRACKING_SYSTEM_BY_HOURLY"


class TrackingSystemV2Dag(DagMaker):
    def __init__(self, prefix=DAG_ID):
        super().__init__(prefix=prefix, name_params=["table_id"])


def create_dag(dag_id, module, event, table_id):
    logging.info(f"tracking_system_dynamic_dag- create_dag:{module} {event} ")
    dag, ops = TrackingSystemV2Dag(prefix=dag_id).make_dag(module=module, event=event, table_id=table_id)
    logging.info(f"tracking_system_dynamic_dag- DAG:{dag} end create_dag")
    return dag
