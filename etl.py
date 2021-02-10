import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 Buckets to Readshift database staging tables"""
    for query in copy_table_queries:
        print(f"Inserting staging data using: {query}")
        cur.execute(query)
        conn.commit()
    print("Staging data inserting: Done")


def insert_tables(cur, conn):
    """Insert data from Readshift staging tables to fact/dimensional tables"""
    for query in insert_table_queries:
        print(f"Running fact/dimensional data inserting using: {query}")
        cur.execute(query)
        conn.commit()
    print(f"Fact/dimensional data inserting: Done")


def main():
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

    conn.close()


if __name__ == "__main__":
    main()