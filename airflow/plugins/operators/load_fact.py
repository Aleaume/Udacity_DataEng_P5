from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
            {}
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                
                 redshift_conn_id="",
                 sql="",
                 table = "",
                 truncate = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading the Songplays table')
        
        formatted_sql= LoadFactOperator.insert_sql.format(
            self.table,
            self.sql
        )
        if self.truncate:
            redshift.run("""truncate {}""".format(self.table))
            redshift.run(formatted_sql)
        else:
            redshift.run(formatted_sql)