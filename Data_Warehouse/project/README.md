# Sparkify Data Engineering Project

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## System Architecture for AWS S3 to Redshift ETL

<img src = sparkify-s3-to-redshift-etl.png width = 500></img>

## Implementation

## Setup

### Local Repo & Virtual Environment
1. clone this repo.
2. move to the local repo folder.
3. create a virtual environment.
    ```
    python -m venv .venv
    ```
4. activate the virtual environment.
    ```
    source .venv/bin/activate
    ```
5. install required packages
    ```
    pip install -r requirements.txt
    ```
### AWS Setup 
In order to implement this project, you need the following AWS setup. 

1. Register Amazon Web Service.
2. Search IAM.
3. Create User with **AdministratorAccess** permission policy in IAM service.
4. Create **Access Key** in Security Credentials
5. Download or copy **Access Key ID** and **Secret Access Key**.
6. Copy and paste the following content on a new file and name it `dwh.cfg` on the local repo folder 7. Add the values of `access key id` and `secret access key`(from 4.) on `[AWS]`'s KEY and SECRET.
    ```
    [AWS]
    KEY=
    SECRET=

    [DWH] 
    DWH_CLUSTER_TYPE=multi-node
    DWH_NUM_NODES=4
    DWH_NODE_TYPE=dc2.large

    DWH_IAM_ROLE_NAME=dwhRole
    DWH_CLUSTER_IDENTIFIER=dwhCluster 
    DWH_DB=dwh
    DWH_DB_USER=dwhuser
    DWH_DB_PASSWORD=
    DWH_PORT=5439
    DWH_IAM_ROLE_ARN=

    [CLUSTER]
    DWH_ENDPOINT=
    DWH_DB=dwh
    DWH_DB_USER=dwhuser
    DWH_DB_PASSWORD=
    DWH_PORT=5439
    ```
8. Create AWS resources by running the following command.
    ```
    python aws_resource_setup_IaC.py
    ```
7. Copy `cluster endpoint`, `cluster role arn` from the output.
8. Open `dwh.cfg` file and paste each values on `DWH_ENDPOINT` and `DWH_IAM_ROLE_ARN`.

## Database Schema Design

Next, create the tables by running the following command on terminal.
```
python create_tables.py
```
[CAUTION] It will drop the existing tables under the same names on Redshift before loading.

### Staging Tables on Redshift

#### 1. staging_events
   - Columns: artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId
   - Purpose: Stores raw data from user events on the Sparkify platform.

#### 2. staging_songs
   - Columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year
   - Purpose: Stores raw data about songs and artists.

### Analytics Tables on Redshift

#### 1. songplays
   - Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
   - Purpose: Records of user song plays, including information about the song, artist, and user session.

#### 2. users
   - Columns: user_id, first_name, last_name, gender, level
   - Purpose: Information about Sparkify users.

#### 3. songs
   - Columns: song_id, title, artist_id, year, duration
   - Purpose: Information about songs in the Sparkify database.

#### 4. artists
   - Columns: artist_id, name, location, latitude, longitude
   - Purpose: Information about artists in the Sparkify database.

#### 5. time
   - Columns: start_time, hour, day, week, month, year, weekday
   - Purpose: Timestamps of user song plays broken down into different time units for analysis.

## ETL Pipeline

### etl.py
Next step is to load data into staging tables and analytics tables by running the following command on terminal.
```
python etl.py
```

#### 1. Loading data from S3 to Staging Tables on Redshift
   - Reads data from JSON files in the S3 bucket and loads it into staging tables (staging_events and staging_songs) on Redshift.

#### 2. Loading data from Staging Tables to Analytics Tables on Redshift
   - Transforms and loads data from staging tables into the analytics tables (songplays, users, songs, artists, time) on Redshift.

### Testing

1. Run `create_tables.py` to create the necessary tables on Redshift.
2. Execute `etl.py` to run the ETL pipeline and populate the analytics tables.
3. Run analytic queries on the Redshift database to compare the results with expected outcomes.

### Example Queries for Song Play Analysis

1. **Most Played Song:**
   ```sql
    SELECT songs.title, artists.name, COUNT(*) AS play_count
    FROM songplays
    JOIN songs ON songplays.song_id = songs.song_id
    JOIN artists ON songs.artist_id = artists.artist_id
    GROUP BY songs.title, artists.name
    ORDER BY play_count DESC
    LIMIT 1;

2. Most Used Hour for Last 7 Days
    ```sql
    WITH last_date_table AS (
    SELECT MAX(songplays.start_time) AS last_date
    FROM songplays
    ),
    start_date_table AS (
    -- BETWEEN function below is inclusive, so used '-6' as interval
    SELECT DATEADD(day, -6, CAST(last_date AS datetime)) AS start_date
    FROM last_date_table
    ),
    last_7days_songplays AS(
    SELECT * FROM songplays, start_date_table, last_date_table 
    WHERE start_time BETWEEN start_date AND last_date
    )
    SELECT t.hour, COUNT(*) AS play_count FROM last_7days_songplays seven
    JOIN time t ON seven.start_time = t.start_time
    GROUP BY t.hour
    ORDER BY play_count DESC
    LIMIT 5;

3. Users' Web browser
    ```sql
    SELECT DISTINCT(split_part(split_part(useragent, '/', 4), ' ', 2)) AS web_browser, 
           COUNT(*) AS num_event
    FROM songplays
    GROUP BY web_browser
    ORDER BY num_event DESC
    LIMIT 3;
