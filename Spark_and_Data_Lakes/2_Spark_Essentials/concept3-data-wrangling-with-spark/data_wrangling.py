import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc, udf
from pyspark.sql.functions import sum as Fsum

import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession \
        .builder \
        .appName('Wrangling Data') \
        .getOrCreate()

# read the json file into spark dataframe
path = '../../data/sparkify_log_small.json'
user_log_df = spark.read.json(path)

# print 5 records
print(
    user_log_df.take(5)
)

# print the schema inferred by pyspark
print('Schema \n')
user_log_df.printSchema()

# describe the dataframe
print('Describe Dataframe \n')
user_log_df.describe().show()

# describe the statitics for the song length column
print('Describe the statistics for the song length \n')
user_log_df.describe('length').show()

# count the rows in the dataframe
print('The number of rows \n')
print(
    user_log_df.count()
)

# select the page column, drop the duplicates, and sort by page
print('Select the column, \'page\' and drop the duplicates \n')
user_log_df.select('page').dropDuplicates().sort('page').show()

# select data for all pages where userId is 1046
print('Select the rows where userID is 1046 ')
user_log_df.select(['userId', 'firstname', 'page', 'song']) \
            .where(user_log_df.userId == '1046') \
            .show()


# # calculate statistics by Hour

# user-defined function
#a lambda function that takes a timestamp x as input and converts it to a datetime object by using the fromtimestamp function from the datetime module. The division by 1000.0 is likely used to convert the timestamp from milliseconds to seconds.
#Finally, .hour is used to extract the hour component from the resulting datetime object.
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).hour)

# add the column called 'hour' with the values 
# from the result of applying get_hour function
user_log_df = user_log_df.withColumn('hour', get_hour(user_log_df.ts))

print('Get the first row \n')
print(
    user_log_df.head(1)
)


# # select just the NextSong page
# songs_in_hour_df = user_log_df.filter(user_log_df.page == 'NextSong')\
#         .groupby(user_log_df.hour) \
#         .count() \
#         .orderBy(user_log_df.hour.cast('float'))

# # convert to Pandas DataFrame
# songs_in_hour_pd = songs_in_hour_df.toPandas()
# # convert hour to numeric value
# songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

# # display as graph
# plt.scatter(songs_in_hour_pd['hour'], songs_in_hour_pd['count'])
# plt.xlim(-1, 24)
# plt.ylim(0, 1.2 * max(songs_in_hour_pd['count']))
# plt.xlabel('Hour')
# plt.ylabel('Songs played')
# plt.show()


# # drop rows with missing values
user_log_valid_df = user_log_df.dropna(how = 'any',
                                       subset = ['userId', 'sessionId'])

print('how many are there now that we dropped rows with null userId or session Id? \n')
print(user_log_valid_df.count())

# select all unique user Ids into a dataframe
user_log_valid_df.select('userId') \
            .dropDuplicates() \
            .sort('userId').show()

# dropna() doesn't workk on empty string, 
# so remove rows with empty string this way
user_log_valid_df = user_log_valid_df.filter(user_log_valid_df['userId'] != '')



# # users downgrade their accounts

user_log_valid_df.filter("page = 'Submit Downgrade'") \
                .show()

user_log_valid_df.select(['userId', 'firstname', 'page', 'level', 'song']) \
                 .where(user_log_valid_df.userId == '1138') \
                 .show()

# create an user-defined funciton
flag_downgrade_event = udf(lambda x: 1 if x == 'Submit Downgrade' else 0, IntegerType())

# select data including the user-defined function
user_log_valid_df = user_log_valid_df.withColumn('downgraded', flag_downgrade_event("page"))

print('Get the first row \n')
print(
    user_log_valid_df.head(1)
)


from pyspark.sql import Window



# partition by user Id
windowval = Window.partitionBy('userId').orderBy(desc('ts')) \
                  .rangeBetween(Window.unboundedPreceding, 0)

user_log_valid_df = user_log_valid_df.withColumn('phase', Fsum('downgraded').over(windowval))
user_log_valid_df.select(['userId', 'firstname', 'ts', 'page', 'level', 'phase']) \
                .where(user_log_df.userId == '1138') \
                .sort('ts').show()