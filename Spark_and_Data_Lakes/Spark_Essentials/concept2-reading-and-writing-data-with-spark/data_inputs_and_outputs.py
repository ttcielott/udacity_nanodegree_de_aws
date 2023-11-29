import findspark
findspark.init()

from pyspark.sql import SparkSession

# the session is just for development
# because we are not running on a spark cluster

spark = SparkSession \
        .builder \
        .appName('Our first Python Spark SQL example') \
        .getOrCreate()

# this should print the default configuration
print(
    f'Default Configuration: \n {spark.sparkContext.getConf().getAll()} \n'
)

# this path resides on your computer or workspace,
# not in HDFS
path = '../../data/sparkify_log_small.json'
# get DataFrame (which has a lot more advanced capabilities than RDDs)
user_log_df = spark.read.json(path)

# see how Spark inferred the schema from the JSON file
print('Schema inferred by Spark \n')
user_log_df.printSchema()
print(
    f'\n Output of .describe(): \n {user_log_df.describe()} \n'
)

print('Output of .show(n=1) \n')
user_log_df.show(n=1)
# take a sample of five records from the DataFrame
print('Output of .take(5)\n')
print(
    user_log_df.take(5)
)

# change file formats (unstructured or semi-structured data)
out_path = '../../data/sparkify_log_small.csv'

# the filename alone didn't tell Spark the actual format,
# we need to do it here
user_log_df.write.mode('overwrite').save(out_path, format = 'csv', header = True)

# Notice we have created another dataframe here
# Let's read the file we just wrote to see the result
user_log_2_df = spark.read.csv(out_path, header = True)
print('\n Schema inferred by Spark \n')
user_log_2_df.printSchema()

# choose two records from the CSV file
print('Output of .take(2) \n')
print(
    user_log_2_df.take(2)
)

# show the userID column for the first several rows
print('Output of .select("userID").show() \n')
user_log_2_df.select('userID').show()

print('Output of .take(1) \n')
print(
    user_log_2_df.take(1)
)
