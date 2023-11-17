# Lesson 4: Using the WHERE Claue
# User partition key and clustering columns in order defined as primary key contraint of table creation 

# create a connection to the cassandra database
from cassandra.cluster import Cluster

try:
    cluster = Cluster(['0.0.0.0'])
    session = cluster.connect()
    print('cassandra db connected\n')
except Exception as e:
    print(e)

# create a keyspace
try:
    cql = '''CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 
                        'replication_factor' : 1}
    '''
    session.execute(cql)
    print('keyspace created\n')
except Exception as e:
    print(e)

# connect to the keyspace
try:
    session.set_keyspace('udacity')
    print('keyspace connected\n')
except Exception as e:
    print(e)

# Let's imagine we would like to start creating a new Music Library of albums. 
# We want to ask 4 question of our data

# 1. Give me every album in my music library that was released in a 1965 year
## query : SELECT * FROM music_library WHERE year = 1965;(although this kind of query is highly discouraged for big data)

# 2. Give me the album that is in my music library that was released in 1965 by 'The Beatles';
## query : SELECT * FROM music_library WHERE year = 1965 AND artist_name = 'The Beatles';

# 3. Give me all the albums released in a given year that was made in London 
## query : SELECT * FROM music_library WHERE year = 1965 AND city = 'London';

# 4. Give me the city that the album "Rubber Soul" was recorded
## query : SELECT city FROM music_library WHERE album_name = 'Rubber Soul';

