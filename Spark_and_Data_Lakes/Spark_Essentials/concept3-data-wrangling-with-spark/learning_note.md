# Learning Note

## General functions

1. `where()`: is just an alias for `filter()`
2. `withColumn()`: returns a new DataFrame by adding a column or replacing the existing column that has the same name. The first parameter is the name of the new column, the second is an expression of how to compute it. 

## Difference between .show() and print() in pyspark

1. `.show()`: This method is specific to PySpark and is used to display the contents of a DataFrame or RDD in a tabular format. It provides a more structured and visually appealing output. By default, it shows the first 20 rows of the DataFrame. You can specify the number of rows to display by passing an argument to the `show()` method like `df.show(10)`.
2. `print()`: This is a built-in Python function that can be used to display the contents of any object. However, unlikke `.show()`, `print()` does not provide a tabular format. It simply displays the elements of the object as text, one element per line. If you try to print a large DataFrame or RDD, it may not be displayed completely, and only a summany of the data will be shown.

## Aggregate functions

Spark SQL provides built-in methods for the most common aggregations such as `count()`, `countDistict()`, `avg()`, `max()`, `min()`, etc. in the pyspark.sql.functions module. These methods are not the same as the built-in the Python Standard Library.

## User defined functions (UDF)

In Spark SQL we can define our own functions with the udf method from the pyspark.sql.functions module. The default type of the return variable for UDFs is string. If you would like to return an other type we need to explicitly do so by using the different types from the pyspark.sql.types module.

## Window functions

Window functions are a way of combining the values of rows in a DataFrame. When defining the window we can choose how to sort and group the rows (`partitionBy`) and how wide of a window we'd like to use (`rangeBetween` or `rowsBetween`).
