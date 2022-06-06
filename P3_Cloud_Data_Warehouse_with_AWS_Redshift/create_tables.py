import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    Runs 'DROP TABLE' queries
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
    Runs 'CREATE TABLE' queries
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Main function for the creation of tables in AWS redshift: 
        1. Reads the AWS credentials from the dwh.cfg
        2. Creates the psycopg2 connection and the cursor object
        3. Drops old tables and creates new tables, utilizing the drop_tables and create_tables functions
        4. Closes the connection to the AWS Redshift server
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()