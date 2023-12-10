# This is the same data wrangling we performed with DataFrames in concept 3, 
# but this time we will use a declarative approach with SQL (much simpler!)
from pyspark.sql import SparkSession
import datetime

spark = SparkSession \
        .builder \
        .appName('Data wrangling with Spark SQL') \
        .getOrCreate()

path = '../../data/sparkify_log_small.json'
user_log = spark.read.json(path)

user_log.take(1)

user_log.printSchema()

# # Create a View and Run Queries
#
# The code below creates a temporary view against which you can run SQL queries.
# This is a part of Spark SQL
# It doesn't last after this job's done
user_log.createOrReplaceTempView('user_log_table')

# use the .sql() method of the spark object to run queries against the view
# show the first 2 lines of the table
spark.sql(
    '''
    SELECT *
    FROM user_log_table
    LIMIT 2
    '''
).show()

# count the row number of the table
spark.sql(
    '''
    SELECT COUNT(*)
    FROM user_log_table
    '''
).show()

# view the row of a specific userID
spark.sql(
    '''
    SELECT userId, firstname, page, song
    FROM user_log_table
    WHERE userId = '1046'
    '''
).show()

# show distinct values of page
spark.sql(
    '''
    SELECT DISTINCT page
    FROM user_log_table
    ORDER BY page
    '''
).show()

# # user defined functions
# define the function that converts milliseconds to hours
spark.udf.register('get_hour', 
                   lambda x: int(datetime.datetime.fromtimestamp(x/1000.0).hour))
songs_in_hour = spark.sql(
    '''
    SELECT get_hour(ts) AS hour, COUNT(*) AS plays_per_hour
    FROM user_log_table
    WHERE page = "NextSong"
    GROUP BY hour
    ORDER BY cast(hour as int) 
    -- cast hour as integer because the default output of udf is a string
    '''
)

# # Converting Results to Pandas
songs_in_hour_pd = songs_in_hour.toPandas()

print(songs_in_hour_pd)
