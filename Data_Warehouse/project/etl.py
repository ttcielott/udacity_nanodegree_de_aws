import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    The function `load_staging_tables` executes a series of SQL queries to load data into staging tables
    and prints a message indicating the completion of each query.
    
    Args:
      cur: The "cur" parameter is a cursor object that is used to execute SQL queries and fetch results
    from the database.
      conn: The `conn` parameter is a connection object that represents the connection to the database.
    It is used to establish a connection to the database and execute SQL queries.
    """
    for query_name, query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'{query_name} is DONE')


def insert_tables(cur, conn):
    """
    The function `insert_tables` executes a series of insert queries, commits the changes to the
    database, and prints the query name and row count.
    
    Args:
      cur: The "cur" parameter is a cursor object that is used to execute SQL queries and fetch results
    from the database.
      conn: The `conn` parameter is a connection object that represents the connection to the database.
    It is used to establish a connection to the database and execute SQL queries.
    """
    for query_name, insert_query, check_row_num_query in insert_table_queries:
        cur.execute(insert_query)
        conn.commit()
        print(f'{query_name} is DONE')

        cur.execute(check_row_num_query)
        row_num = cur.fetchone()[0]
        print(f'{query_name} row count: {row_num}')


def check_load_error(cur):
    """
    The function `check_load_error` retrieves the most recent 5 load errors from the `stl_load_errors`
    table on Redshift and prints them in a tabular format.
    
    Args:
      cur: The parameter `cur` is a cursor object that is used to execute SQL queries and fetch the
    results from the database. It is typically obtained by connecting to a database and creating a
    cursor object using the appropriate database driver.
    """
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
    """
    The main function connects to a Redshift database using the configuration values from a file,
    loads staging tables, inserts data into tables, and then closes the connection.
    """
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