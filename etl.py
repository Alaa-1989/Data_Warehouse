import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    """
        Function for queries to loads data from S3 buckets to Redshift.
        Arguments: 
        cur: the cursor object of the database.
        conn: to connect to the server.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):           
    """
        Function for insert statements from staging tables to the DIM and FACT tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Function for:
        - Reads the database credentials from the config file.
        - Connect to the database.
        - Loads the S3 buckets into staging tables. 
        - Loads final tables from staging tables.
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