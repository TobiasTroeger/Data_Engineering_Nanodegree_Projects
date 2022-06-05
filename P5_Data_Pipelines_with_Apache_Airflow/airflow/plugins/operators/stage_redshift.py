from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Function copies the data of a S3 Bucket, creates the staging table in AWS Redshift if it does not already exists 
    and loads the data into the staging table. Should be old entries in the table, the table is cleared before new data is loadet.
    
    Parameters:
    
        # redshift_conn_id -    credentials to connect with AWS Redshift
        # aws_credentials -     IAM login credentials
        # table -               target fact table
        # create_table_query -  SQL query that creates the corresponding table
        # region -              AWS region of the target S3 bucket. Default: us-west-2
        # s3_bucket -           adress of the target bucket
        # s3_key -              folder structure
        # json_format -         formats the json files in the target folder using the 'auto' pattern or a custome one
    """
    
    ui_color = '#358140'

    copy_from_s3 = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    REGION AS '{}'
                    FORMAT AS json '{}'
                   """    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 create_table_query="",
                 region="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args, **kwargs):

        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.create_table_query = create_table_query
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        credentials = aws_hook.get_credentials()
        
        redshift_hook.run(self.create_table_query)
        entries = redshift_hook.get_records('SELECT COUNT(*) FROM {}'.format(self.table))[0][0]
        
        if entries > 0:
            
            redshift_hook.run('DELETE FROM {};'.format(self.table))
            self.log.info('Successfully deleted old data from {}.'.format(self.table))
                     
        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)
        sql = StageToRedshiftOperator.copy_from_s3.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format
        )
            
        redshift_hook.run(sql) 
        self.log.info('Staging of {} successfull.'.format(self.table))
