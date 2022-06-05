from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    """
    Loads and transforms data from both staging tables and inserts them into the dimension tables 
    
    Parameters:
    
      # redshift_conn_id -  ID to connect with AWS Redshift
      # table -             target dimensional table
      # sql -               corresponding sql query from sql_queries.py

    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table_query="",
                 sql="",
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_query = create_table_query
        self.sql = sql

    def execute(self, context):
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift_hook.run(self.create_table_query)
        entries = redshift_hook.get_records('SELECT COUNT(*) FROM {}'.format(self.table))[0][0]
        
        if entries > 0:
                
                redshift_hook.run('TRUNCATE TABLE {};'.format(self.table))
                self.log.info('Successfully deleted old data from {}.'.format(self.table))
                
                insert_query ="""
                                INSERT INTO {} {};
                              """.format(
                                         self.table,
                                         self.sql
                                        )
                
        else:                                  
            insert_query = """
                            INSERT INTO {} {};
                           """.format(
                                    self.table,
                                    self.sql
                                    )
            
            
        redshift_hook.run(insert_query)            
        self.log.info('Creation of {} successfull.'.format(self.table))
