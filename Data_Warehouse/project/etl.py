import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query_name, query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'{query_name} is DONE')


def insert_tables(cur, conn):
    for query_name, query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'{query_name} is DONE')

def check_load_error(cur):
    query = """
        select  starttime, raw_line, err_reason
        from stl_load_errors
        order by starttime desc
        limit 5;
    """
    cur.execute(query)
    response = cur.fetchall()
    print(pd.DataFrame(response, columns = ['start_time', 'raw_line', 'error_reason']))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    # check_load_error(cur)

    conn.close()


if __name__ == "__main__":
    main()