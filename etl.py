""" This script is used to load data from S3 into staging tables on Redshift"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, queries_analytics


def load_staging_tables(cur, conn):
    """Load data from S3 into staging tables on Redshift."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into analytics tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def run_queries(cur, conn):
    """Run queries to check data quality."""
    for query in queries_analytics:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to load data from S3 into staging tables on Redshift."""
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
