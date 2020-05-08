import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads data to the staging tables stored in AWS Redshifts
    Parameters:
        cur (psycopg2.cur()): This cursor object is passed to the function to execute SQL commands
        conn (psycopg2.connect()): This object is passed to the function to commit SQL commands
    Return:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts data from stagging tables to final tables in AWS Redshifts
    Parameters:
        cur (psycopg2.cur()): This cursor object is passed to the function to execute SQL commands
        conn (psycopg2.connect()): This object is passed to the function to commit SQL commands
    Return:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # parse data and connect the the database. 
    #  Plesea create on AWS Redshift a cluster and a database before running this file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
