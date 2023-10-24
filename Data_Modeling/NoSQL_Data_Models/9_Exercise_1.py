# Apahche Cassandra
# Lesson 3 Exercise 1: Three Queries Three Tables

# please follow through README.md in this folder
# import Apache Cassandra python package
# create a connection to the database
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['0.0.0.0'])
    session = cluster.connect()
    print('cassandra connected!')
except Exception as e:
    print(e)

# create a keyspace to work in
try:
    cql = """
        CREATE KEYSPACE IF NOT EXISTS udacity
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor': 1 };
    """
    session.execute(cql)
    print('keyspace created!')
except Exception as e:
    print(e)

try:
    session.set_keyspace('udacity')
    print('keyspace connected!')
except Exception as e:
    print(e)

# question 1: Give every album in the music library that was released in a given year
# query example : SELECT * FROM music_library WHERE year = 1970
# create the table
cql = """CREATE TABLE IF NOT EXISTS music_library
         (year int, artist_name varchar, album_name varchar, PRIMARY KEY(year, artist_name));
"""
try:
    session.execute(cql)
    print('music library table created!')
except Exception as e:
    print(e)

# question 2: Give every album in the music library that was created by a given artist
# query example : select * from artist_library WHERE artist_name="The Beatle
# create the table
cql = """CREATE TABLE IF NOT EXISTS artist_library
         (year int, artist_name varchar, album_name varchar, PRIMARY KEY(artist_name, year));
"""
try:
    session.execute(cql)
    print('artist library table created!')
except Exception as e:
    print(e)

# question 3: Give all the information from the music library about a given album
# query example : select * from album_library WHERE album_name="Close To You"
# create the table
cql = """CREATE TABLE IF NOT EXISTS album_library
         (year int, artist_name varchar, album_name varchar, PRIMARY KEY(album_name, artist_name));
"""
try:
    session.execute(cql)
    print('album library table created!')
except Exception as e:
    print(e)


# insert values into the table
cql1 = """INSERT INTO music_library
         (year, artist_name, album_name)
         VALUES (%s, %s, %s);
"""
cql2 = """INSERT INTO artist_library
         (artist_name, year, album_name)
         VALUES (%s, %s, %s);
"""
cql3 = """INSERT INTO album_library
         (album_name, artist_name, year)
         VALUES (%s, %s, %s);
"""
try:
    session.execute(cql1, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(cql1, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(cql1, (1965, "The Who", "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(cql1, (1966, "The Monkees", "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(cql1, (1970, "The Carpenters", "Close To You"))
except Exception as e:
    print(e)
print('music library table all inserted!')

try:
    session.execute(cql2, ("The Beatles", 1970, "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(cql2, ("The Beatles", 1965, "Rubber Soul"))
except Exception as e:
    print(e)
    
try:
    session.execute(cql2, ("The Who", 1965, "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(cql2, ("The Monkees", 1966, "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(cql2, ("The Carpenters", 1970, "Close To You"))
except Exception as e:
    print(e)
print('artist library table all inserted!')
    
try:
    session.execute(cql3, ("Let it Be", "The Beatles", 1970))
except Exception as e:
    print(e)
    
try:
    session.execute(cql3, ("Rubber Soul", "The Beatles", 1965))
except Exception as e:
    print(e)
    
try:
    session.execute(cql3, ("My Generation", "The Who", 1965))
except Exception as e:
    print(e)

try:
    session.execute(cql3, ("The Monkees", "The Monkees", 1966))
except Exception as e:
    print(e)

try:
    session.execute(cql3, ("Close To You", "The Carpenters", 1970))
except Exception as e:
    print(e)
print('album library table all inserted!')

# validate the data model
cql = "SELECT * FROM music_library WHERE year = 1970;"
try:
    rows = session.execute(cql)
except Exception as e:
    print(e)
for row in rows:
    print(row.year, row.artist_name, row.album_name)

# validate the data model
cql = "SELECT * FROM artist_library WHERE artist_name = 'The Beatles';"
try:
    rows = session.execute(cql)
except Exception as e:
    print(e)
for row in rows:
    print(row.artist_name, row.year, row.album_name)

# validate the data model
cql = "SELECT * FROM album_library WHERE album_name = 'Close To You';"
try:
    rows = session.execute(cql)
except Exception as e:
    print(e)
for row in rows:
    print(row.artist_name, row.year, row.album_name)

# close the session and cluster connection
session.shutdown()
cluster.shutdown()