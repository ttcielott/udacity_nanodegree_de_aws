# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, sum as Fsum
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


# 