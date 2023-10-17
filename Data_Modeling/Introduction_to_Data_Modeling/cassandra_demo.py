from cassandra.cluster import Cluster

# instantiate a cluster class
# we are running node locally, so the input is '0.0.0.0'
try:
    cluster = Cluster(['0.0.0.0'], port = 9042)
    # connect to the cluster created
    session = cluster.connect()
except Exception as e:
    print(e)

# create a keyspace
query = """
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """
try:
    session.execute(query)
except Exception as e:
    print(e)

# connect to the keyspace created
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# create a table
query = "CREATE TABLE IF NOT EXISTS music_library"
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# insert two rows
query = "INSERT INTO music_library (year, artist_name, album_name)"
query = query + "VALUES (%s, %s, %s)"
try:
    session.execute(query, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)
try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)

# validate the data was inserted into the keyspace
query = "SELECT * FROM music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.album_name, row.artist_name)