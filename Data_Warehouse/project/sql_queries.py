import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_IAM_ROLE_ARN = config.get("DWH", "DWH_IAM_ROLE_ARN")
SONG_DATA_LINK = config.get("DATA", "SONG_DATA_LINK")
LOG_DATA_LINK = config.get("DATA", "LOG_DATA_LINK")
LOGMETA_DATA_LINK = config.get("DATA", "LOGMETA_DATA_LINK")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist          VARCHAR(255),
    auth            VARCHAR(255), 
    firstName       VARCHAR(255),
    gender          VARCHAR(255),   
    itemInSession   INTEGER,
    lastName        VARCHAR(255),
    length          FLOAT,
    level           VARCHAR(255), 
    location        VARCHAR(255),
    method          VARCHAR(255),
    page            VARCHAR(255),
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR(255),
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR(255),
    userId          INTEGER
);""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER ,
    artist_id VARCHAR(255),
    artist_latitude VARCHAR(255),
    artist_longitude VARCHAR(255),
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(255),
    title VARCHAR(255),
    duration REAL,
    year INTEGER 
);""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id VARCHAR(255),
    start_time timestamptz sortkey,
    user_id INTEGER,
    level VARCHAR(255),
    song_id VARCHAR(255),
    artist_id VARCHAR(255),
    sessionId INTEGER,
    location VARCHAR(255),
    userAgent VARCHAR
);""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(255),
    level VARCHAR
) diststyle all;""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(255),
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year INTEGER  sortkey,
    duration REAL
) diststyle all;""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(255),
    name VARCHAR(255),
    location VARCHAR(255),
    latitude VARCHAR(255),
    longitude VARCHAR
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
SONG_DATA_LINK = config.get("DATA", "SONG_DATA_LINK")
LOG_DATA_LINK = config.get("DATA", "LOG_DATA_LINK")
LOGMETA_DATA_LINK = config.get("DATA", "LOGMETA_DATA_LINK")
staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role '{}'
json {}
emptyasnull
blanksasnull
;""").format(LOG_DATA_LINK, DWH_IAM_ROLE_ARN, LOGMETA_DATA_LINK )

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role '{}'
json 'auto'
emptyasnull
blanksasnull
;""").format(SONG_DATA_LINK, DWH_IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
    songplay_id, start_time, user_id, level, song_id, 
    artist_id, sessionId, location, userAgent
    )
    SELECT e.page, 
            timestamp with time zone 'epoch' + e.ts/1000 * interval '1 second',
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
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) AS dayofweek
    FROM songplay
;""")

# CHECK ROW COUNT
def check_row_count(table_name):
    """
    The function returns a SQL query that counts the number of rows in a specified table.
    
    Args:
      table_name: The table_name parameter is a string that represents the name of the table for which
    you want to check the row count.
    
    Returns:
      The SQL query string that counts the number of rows in the specified table.
    """
    row_count = """
    SELECT COUNT(*) FROM {}
    """.format(table_name)

    return row_count

# QUERY LISTS

create_table_queries = [
                        ('staging_events_table_create', staging_events_table_create),
                        ('staging_songs_table_create', staging_songs_table_create),
                        ('songplay_table_create', songplay_table_create), 
                        ('user_table_create', user_table_create), 
                        ('song_table_create', song_table_create), 
                        ('artist_table_create', artist_table_create), 
                        ('time_table_create', time_table_create)
                        ]

drop_table_queries = [
                      ('staging_events_table_drop', staging_events_table_drop),  
                      ('staging_songs_table_drop', staging_songs_table_drop),  
                      ('songplay_table_drop', songplay_table_drop),  
                      ('user_table_drop', user_table_drop),  
                      ('song_table_drop', song_table_drop), 
                      ('artist_table_drop', artist_table_drop),  
                      ('time_table_drop', time_table_drop)
                      ] 

copy_table_queries = [
                        ('staging_events_copy', staging_events_copy), 
                        ('staging_songs_copy', staging_songs_copy)
                      ]

insert_table_queries = [
                        ('songplay_table_insert', songplay_table_insert, check_row_count('songplays')), 
                        ('user_table_insert', user_table_insert, check_row_count('users')), 
                        ('song_table_insert', song_table_insert, check_row_count('songs')), 
                        ('artist_table_insert', artist_table_insert, check_row_count('artists')), 
                        ('time_table_insert', time_table_insert, check_row_count('time'))
                        ]
