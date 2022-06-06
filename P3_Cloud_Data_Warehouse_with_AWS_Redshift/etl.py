import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Loads Data from S3 bucket into both staging tables
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    Load data from both staging tables and inserts them into fact- and dimensional tables
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Runs the ETL pipeline:
        1. Reads the AWS credentials from the dwh.cfg
        2. Creates the psycopg2 connection and the cursor object
        3. Loads data from S3 buckets and loads them into the staging tables with 'load_staging_tables' function
        4. Loads data from staging tables and inserts them into 1 fact and 4 dimensional tables with 'insert_tables'
        4. Closes the connection to the AWS Redshift server 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()