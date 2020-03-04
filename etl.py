import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    perform ETL operations to bring data from the raw 
    dataset in S3 to the staging tables of the cluster in redshift. 
    
    Parameters: 
        cur(redshift cursor): represent the redshift cursor obtained in the main function
        conn(redhisft connection): represent the redshift connection obtained in the main function
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    perform ETL operations to bring data from the staging
    tables in the cluster to the dimensional and fact tables in the same cluster.  
    
    Parameters: 
        cur(redshift cursor): represent the redshift cursor obtained in the main function
        conn(redhisft connection): represent the redshift connection obtained in the main function
    """ 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        

def main():
    """
    Main function where the connection to the database is being created 
    and the call to the functions that perform the ETL also is being done here
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