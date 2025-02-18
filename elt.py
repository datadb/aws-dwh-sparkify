import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from S3 to staging tables using copy_table_queries.

    Args:
        cur: The database cursor object.
        conn: The database connection object.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from staging tables to final tables using insert_table_queries.

    Args:
        cur: The database cursor object.
        conn: The database connection object.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to load staging tables and insert data into final tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
