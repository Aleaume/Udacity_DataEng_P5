from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
            INSERT INTO {}
            {}
       """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table = "",
                 sql = "",
                 truncate = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        log = "Dimension table {} loaded.".format(self.table) 
        self.log.info(log)
        
        formatted_sql= LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql
        )
        if self.truncate:
            redshift.run("""truncate {}""".format(self.table))
            redshift.run(formatted_sql)
        else:
            redshift.run(formatted_sql)
      
