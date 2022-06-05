from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
    Checks data quality by running a test query and compares the result with the expected value.
    
    Parameters:
    
      # redshift_conn_id -  ID to connect with AWS Redshift
      # test_sql_query -    SQL test query
      # exp_result -        expected result for comparison with the result of the test query
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_sql_query="",
                 exp_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_sql_query = test_sql_query
        self.exp_result = exp_result
        
    def execute(self, context):
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        test_result = redshift_hook.get_records(self.test_sql_query)[0][0]
        
        self.log.info('Result of test SQL query is {}'.format(test_result))
        if test_result == self.exp_result:
            self.log.info('Test result and expected value are identical. Quality check passed')
            
        else:
            self.log.info('Quality check not passed. No match between {} and {}.'.format(test_result,
                                                                                         self.exp_result))
