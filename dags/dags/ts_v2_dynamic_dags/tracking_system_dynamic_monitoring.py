import logging
from airflow.models import DAG
from edo.dagger.utils.dag_maker_utils import generate_subdag_dict_from_url

from dags.ts_v2_dynamic_dags import create_dag

SUBDAGS_ID = "TS_V2_MONITORING"

modules = generate_subdag_dict_from_url(SUBDAGS_ID)
logging.info(f"tracking_system_dynamic_monitoring- EVENTS len:{str(len(modules))} get list subdags")
for module in modules:
    logging.info(f"tracking_system_dynamic_monitoring- EVENT:{module} Start process")
    for event in module['events']:
        dag_id = f"{SUBDAGS_ID}_{event['table_id']}"
        logging.info(f"tracking_system_dynamic_monitoring- DAG_ID:{dag_id} ")
        logging.info(f"tracking_system_dynamic_monitoring- EVENT:{event} init create dag")
        globals()[dag_id] = create_dag(SUBDAGS_ID,module['module'], event['name'], event['table_id'])
