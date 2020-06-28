import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


    """
        Function to drop table queries and executes them.
        Arguments: 
        cur: the cursor object of the database.
        conn: to connect to the server.
    """
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    """
        Function to create table queries and executes them.
    """        
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        
    """
        Function for:
        - Connect to the database.
        - Drops and creates the required tables.
    """
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()