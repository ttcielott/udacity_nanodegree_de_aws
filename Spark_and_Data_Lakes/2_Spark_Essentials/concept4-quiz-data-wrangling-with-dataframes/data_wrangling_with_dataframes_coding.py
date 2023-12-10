# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, sum as Fsum, desc
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# instantiate a Spark Session
spark = SparkSession \
        .builder \
        .appName('DataFrames practice') \
        .getOrCreate()

# load data as DataFrame
path = '../../data/sparkify_log_small.json'
user_log = spark.read.json(path)

# # Question 1
#
# Which page did user id ""(empty string) NOT visit?
pages_all_user = user_log \
                .select('page') \
                .dropDuplicates()

pages_non_id_user = user_log.filter('userId = ""') \
                            .select('page') \
                            .alias('pages_of_non_user_id_vists') \
                            .dropDuplicates()
for row in set(pages_all_user.collect()) - set(pages_non_id_user.collect()):
    print(row.page)


# # Question 2
#
# What many female users do we have in the dataset?

# select columns we need
user_log.select(['userId', 'gender']) \
        .dropDuplicates() \
        .groupby('gender') \
        .count() \
        .show()


# # Question 3
#
# How many songs were played played from the most played artist?
user_log.filter('page = "NextSong"') \
        .groupby('artist') \
        .agg({'artist': 'count'}) \
        .withColumnRenamed('count(Artist)', 'Playcount') \
        .sort(desc('Playcount')) \
        .show(1)

# Question 4 (challenge)
#
# How many songs do users listen to on average
# between visiting our homepage?
# Please round your answer to the closest integer.
user_window = Window \
            .partitionBy('userId') \
            .orderBy(desc('ts')) \
            .rangeBetween(Window.unboundedPreceding, 0)

isHome= udf(lambda x: int(x == 'Home'), IntegerType())

# filter only NextSong and Home pages, add 1 for each time they visit Home
# (listening songs -> visiting home)
# adding a column period which is a specific interval between Home visits
cusum = user_log.filter((user_log.page == 'NextSong') | (user_log.page == 'Home')) \
        .select(['userId', 'ts', 'page']) \
        .withColumn('homevisit', isHome('page')) \
        .withColumn('period', Fsum('homevisit').over(user_window))

cusum.show(300)

# period : the numbering of home visit
cusum.filter('page == "NextSong"') \
     .groupby(['userId', 'period']) \
     .agg({'period': 'count'}) \
     .agg({'count(period)': 'avg'}) \
     .show()

# see how many songs were listened to on average during each period

