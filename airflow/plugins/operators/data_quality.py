from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    count_sql_staging = """
        SELECT COUNT(*) FROM ({}) X
    
    """
    count_sql_dimfact = """
        SELECT COUNT(*) FROM {}
        
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 insert_query = "",
                 table_star = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query
        self.table_star = table_star

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        sql_formatted_staging = DataQualityOperator.count_sql_staging.format(
                self.insert_query
            )
        sql_formatted_dimfact = DataQualityOperator.count_sql_dimfact.format(
            self.table_star
        )
        records_staging = redshift.get_records(sql_formatted_staging)
        records_dimfact = redshift.get_records(sql_formatted_dimfact)
        log_success = "Quality check on {} passed with {} records".format(self.table_star,records_staging) 
        log_fail = "Quality check on {} failed. {} with {} -- where staging with {}".format(self.table_star, self.table_star,records_dimfact, records_dimfact) 
        
        if records_staging == records_dimfact:
            
            self.log.info(log_success)
        else:
            raise ValueError(log_fail)