from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    """
    Loads data from the staging tables into the songplays fact table.
    
    Parameters:
    
      # redshift_conn_id -  ID to connect with AWS Redshift
      # table -             target fact table
      # sql -               corresponding sql query from sql_queries.py

    """    
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table_query="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_query = create_table_query
        self.sql = sql
        

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        redshift_hook.run(self.create_table_query)
        
        insert_facts_query =  """
                                INSERT INTO {}{}
                              """.format(
                                        self.table,
                                        self.sql
                                        )
        
        redshift_hook.run(insert_facts_query)
        
        self.log.info('Load {} fact table successfull.'.format(self.table))
