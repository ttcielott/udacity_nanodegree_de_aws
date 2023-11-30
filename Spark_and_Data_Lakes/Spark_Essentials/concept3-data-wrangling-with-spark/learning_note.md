# Learning Note

## Difference between .show() and print() in pyspark

1. `.show()`: This method is specific to PySpark and is used to display the contents of a DataFrame or RDD in a tabular format. It provides a more structured and visually appealing output. By default, it shows the first 20 rows of the DataFrame. You can specify the number of rows to display by passing an argument to the `show()` method like `df.show(10)`.
2. `print()`: This is a built-in Python function that can be used to display the contents of any object. However, unlikke `.show()`, `print()` does not provide a tabular format. It simply displays the elements of the object as text, one element per line. If you try to print a large DataFrame or RDD, it may not be displayed completely, and only a summany of the data will be shown.