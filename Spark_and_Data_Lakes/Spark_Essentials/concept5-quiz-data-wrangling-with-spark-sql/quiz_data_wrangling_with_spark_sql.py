from pyspark.sql import SparkSession

# instantiate a Spark Session
spark = SparkSession \
        .builder \
        .appName('data wrangling with spark sql quiz') \
        .getOrCreate()

# read the dataset
path = '../../data/sparkify_log_small.json'
user_log = spark.read.json(path)

# # creat a view to use with the SQL queries
user_log.createOrReplaceTempView('user_log_table')

# # # Question 1
# # 
# # Which page did user id ""(empty string) NOT visit?
# all_pages = spark.sql(
#     '''
#     SELECT DISTINCT page
#     FROM user_log_table
#     '''
# ).collect()
# pages_no_userId_visited = spark.sql(
#     '''
#     SELECT DISTINCT page
#     FROM user_log_table
#     WHERE userId == ""
#     '''
# ).collect()


# # Print the visited pages
# # without .collect() in `all_pages` and `pages_no_userId_visited`
# # the conversion into set doesn't work
# for row in set(all_pages) - set(pages_no_userId_visited):
#     print(row.page)

# # # Question 2 - Reflect
# # 
# # Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?


# # # Question 3
# # 
# # How many female users do we have in the data set?

# spark.sql(
#     '''
#     SELECT gender, COUNT(DISTINCT userId) AS num_users
#     FROM user_log_table
#     WHERE gender = 'F'
#     GROUP BY gender
#     '''
# ).show()


# # # Question 4
# # 
# # How many songs were played from the most played artist?

# spark.sql(
#     '''
#     SELECT artist, COUNT(*) AS played_songs_count
#     FROM user_log_table
#     WHERE page = "NextSong"
#     GROUP BY artist
#     ORDER BY played_songs_count DESC
#     LIMIT 1
#     '''
# )

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
spark.udf.register('ishome', lambda x: int(x == 'Home'))

spark.sql(
    '''
    WITH tellyup AS (SELECT userId, page, ts, 
            CAST(ishome(page) AS INT) AS homeVisit,
            SUM(CAST(ishome(page) AS INT))
            OVER(PARTITION BY  userId
                ORDER BY ts DESC
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS period
    FROM user_log_table
    WHERE page IN ("Home", "NextSong")
    )

    SELECT AVG(played_songs)
    FROM (
        SELECT userId, period, COUNT(*) AS played_songs
        FROM tellyup
        WHERE page = "NextSong"
        GROUP BY userID, period
    )
    '''
).show(300)
