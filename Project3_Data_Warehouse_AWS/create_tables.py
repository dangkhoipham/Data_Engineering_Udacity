import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops all tables before creating new tables with the same name.
    Parameters:
        cur (psycopg2.cur()): This cursor object is passed to the function to execute SQL commands
        conn (psycopg2.connect()): This object is passed to the function to commit SQL commands
    Return:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function createss all final tables in AWS Redshift before inserting data
    Parameters:
        cur (psycopg2.cur()): This cursor object is passed to the function to execute SQL commands
        conn (psycopg2.connect()): This object is passed to the function to commit SQL commands
    Return:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


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
