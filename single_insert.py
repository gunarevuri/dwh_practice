import configparser
import psycopg2
from sql_queries import  user_table_insert, artist_table_insert,  song_table_insert, time_table_insert,songplay_table_insert
# import dwh.cfg



def insert_users(cur, conn,query):
    # insert records to users relation 
	print("query"+str(query))
	cur.execute(query)
	conn.commit()
	print("finish user query insert")

def insert_songs(cur, conn,query):
    # insert records to songs relation
	print("query"+str(query))
	cur.execute(query)
	conn.commit()
	print("finish songs query insert")

def insert_artists(cur, conn,query):
    # insert records to artists relation
	print("query"+str(query))
	cur.execute(query)
	conn.commit()
	print("finish artists query insert")

def insert_time(cur, conn,query):
    # insert records to time relation
	print("query"+str(query))
	cur.execute(query)
	conn.commit()
	print("finish time query insert")

def insert_songplay(cur,conn,query):
    # insert records to songplay relation
	print("query"+str(query))
	cur.execute(query)
	conn.commit()
	print("finish songplay table query")



def main():
	config = configparser.ConfigParser()
	config.read('dwh.cfg')

	conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
	cur = conn.cursor()
	# inserting records
    
	insert_artists(cur, conn, query= artist_table_insert)
	insert_songs(cur, conn, query= song_table_insert)
	insert_time(cur, conn, query = time_table_insert)
	insert_songplay(cur, conn, query = songplay_table_insert)

	conn.close()


if __name__ == "__main__":
	main()