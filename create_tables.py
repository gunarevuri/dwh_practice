import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, insert_table_queries, select_number_rows_queries, create_real_tables


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("query"+str(query))
        cur.execute(query)
        conn.commit()
        print("finish--------------------finish")



def create_tables(cur, conn):
    for query in create_table_queries:
        print("query"+str(query))
        cur.execute(query)
        conn.commit()
        print("finish--------------------finish")


def create_real_tables(cur, conn):
    for query in create_real_tables:
        print("query"+str(query))
        cur.execute(query)
        conn.commit()
        print("-----finish "+str(query)+"----")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    # create_real_tables(cur, conn)

    conn.close()

# def count_rows():
#     config = configparser.ConfigParser()
#     config.read('dwh.cfg')

#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#     cur = conn.cursor()
    
#     for query in select_number_rows_queries:
#         print("query"+str(query))
#         cur.execute(query)
#         conn.commit()

#     conn.close()
    
if __name__ == "__main__":
    main()
#     count_rows()