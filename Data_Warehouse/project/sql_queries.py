import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR(50),
    auth VARCHAR(10),
    firstName VARCHAR(50),
    gener VARCHAR(1),
    itemSession INTEGER,
    lastName VARCHAR(50),
    length REAL,
    level VARCHAR(10),
    location VARCHAR(50),
    method VARCHAR(10),
    page VARCHAR(20),
    registration REAL,
    sessionId INTEGER,
    song VARCHAR(100),
    status INTEGER,
    ts INTEGER,
    userAgent VARCHAR(100),
    userId INTEGER NOT NULL)
;""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER NOT NULL,
    artist_id VARCHAR(50) NOT NULL,
    artist_latitude VARCHAR(20), 
    artist_longitude VARCHAR(20),
    artist_location VARCHAR(100),
    artist_name VARCHAR(20) NOT NULL,
    song_id VARCHAR(20) NOT NULL,
    title VARCHAR(20) NOT NULL,
    duration REAL,
    year INTEGER NOT NULL
);""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id VARCHAR(20) distkey,
    start_time timestamptz sortkey,
    user_id INTEGER,
    level VARCHAR(10),
    song_id VARCHAR(20) NOT NULL,
    artist_id VARCHAR(50),
    sessionId INTEGER,
    location VARCHAR(50),
    userAgent VARCHAR(100)
);""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(1),
    level VARCHAR(10) 
) diststyle all;""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(20) NOT NULL,
    title VARCHAR(20) NOT NULL,
    artist_id VARCHAR(50),
    year INTEGER sortkey,
    duration REAL,
) diststyle all;""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(50),
    name VARCHAR(20),
    location VARCHAR(100),
    latitude VARCHAR(20),
    longitude VARCHAR(20)
) diststyle all;""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamptz sortkey,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
) diststyle all;""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_event 
FROM 's3://udacity-dend/log_data'
iam_role '{}'
json 's3://udacity-dend/log_json_path.json';
;""").format()

staging_songs_copy = ("""
COPY staging_songs
FROM 's3://udacity-dend/song_data'
iam_role '{}'
json 'auto'
;""").format()

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (
    songplay_id, start_time, user_id, level, song_id, 
    artist_id, sessionId, location, userAgent
    )
    SELECT e.page, 
            timestamp with time zone 'epoch' + e.ts * interval '1 second',
            e.userId, e.level, s.song_id,
            s.artist_id, e.sessionId, e.location, e.userAgent
    FROM staging_events e
    INNER JOIN staging_songs s ON e.artist = s.artist_name
                AND e.song = s.title
;""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, first_name, last_name, gender, level
        )
    SELECT userId, firstName, lastName, gender, level
    FROM staging_events
;""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, title, artist_id, year, duration
        )
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs 
;""")

artist_table_insert = ("""
    INSERT INTO artists(
            artist_id, name, location, latitude, longitude
            )
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
;""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, hour, day, week, month, year, weekday
        )
    SELECT start_time,
            date_part(hour, timestamp start_time),
            date_part(day, timestamp start_time),
            date_part(week, timestamp start_time),
            date_part(month, timestamp start_time),
            date_part(year, timestamp start_time),
            date_part(dayofweek, timestamp start_time),
    FROM songplay
;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
