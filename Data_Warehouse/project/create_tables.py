import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    The function `drop_tables` executes a series of SQL queries to drop tables in a database and prints
    a message for each query executed.
    
    Args:
      cur: The `cur` parameter is a cursor object that is used to execute SQL queries and fetch results
    from the database.
      conn: The `conn` parameter is a connection object that represents the connection to the database.
    It is used to establish a connection to the database and execute SQL queries.
    """
    for query_name, query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'{query_name} is DONE')



def create_tables(cur, conn):
    """
    The function `create_tables` executes a series of SQL queries to create tables in a database and
    prints a message for each query indicating its completion.
    
    Args:
      cur: The "cur" parameter is a cursor object that is used to execute SQL queries and fetch results
    from the database.
      conn: The `conn` parameter is a connection object that represents the connection to the database.
    It is used to establish a connection to the database and execute SQL queries.
    """
    for query_name, query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'{query_name} is DONE')


def main():
    """
    The main function connects to a Redshift database, drops existing tables, creates new tables, and
    then closes the connection.
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