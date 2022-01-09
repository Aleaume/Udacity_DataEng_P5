from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#b71c1c'
 

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 tables_list = [""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables_list:
            records_count = redshift.get_records(f"Select count(*) from {table}")[0]

            log_success = "PASSED !! {} returned {} records".format(table, records_count[0]) 
            log_fail = "FAILED !! {} returned {} records".format(table, records_count[0]) 

            if records_count[0] < 1:
                raise ValueError(log_fail)
            else:
                self.log.info(log_success)


