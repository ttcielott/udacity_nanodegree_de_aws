import findspark
findspark.init()

from pyspark.sql import SparkSession

# This sets the name of your Spark application to "Maps and Lazy Evaluation Example". 
# This name will be displayed in the Spark UI and logs.
spark = SparkSession\
    .builder \
    .appName("maps_and_lazy_evaluation_example") \
    .getOrCreate()

# getOrCreat()
# When you're developing and testing your Spark code, 
# it's convenient to use a local SparkSession. 
# This allows you to run and debug your code without the need for a full Spark cluster setup. 
# The local SparkSession runs Spark in a single JVM (Java Virtual Machine) process on your local machine.

# starting off with a regular python list
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]

# paralleise the log_of_songs to use with Spark
distributed_song_log_rdd = spark.sparkContext.parallelize(log_of_songs)

# show the original input data is preserved
# .collect() forces all the data from the entire RDD on all nodes
# .collect() : Sparkk triggers the execution of the DAG and product the final result
# .foreach() allows the data to stay on each of the indepedent nodes
print('[Oringal input]')
distributed_song_log_rdd.foreach(print)

# create a python function to convert strings to lowercase
convert_song_to_lowercase = lambda song: song.lower()


# use the map function to transform the list of songs 
# with the python funciton that converts string to lowercase
lower_case_songs = distributed_song_log_rdd.map(convert_song_to_lowercase)

print('[Afet mapping]')
lower_case_songs.foreach(print)

# show the original input data is preserved
print('[Oringal input]')
distributed_song_log_rdd.foreach(print)