# create_tables.py

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all tables defined in drop_table_queries.

    Args:
        cur: The database cursor object.
        conn: The database connection object.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all tables defined in create_table_queries.

    Args:
        cur: The database cursor object.
        conn: The database connection object.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to drop and create tables in the Redshift cluster."""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
