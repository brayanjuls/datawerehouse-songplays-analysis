import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop all the existing tables from the database
    
    Parameters: 
        cur(redshift cursor): represent the redshift cursor obtained in the main function
        conn(redhisft connection): represent the redshift connection obtained in the main function
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create all the tables needed to perform the ETL operations
    
    Parameters: 
        cur(redshift cursor): represent the redshift cursor obtained in the main function
        conn(redhisft connection): represent the redshift connection obtained in the main function
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function where the connection to the database is being created 
    and the call to the functions that perform the creation of the tables also is being done here
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