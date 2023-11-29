# Spark Essentials

For the implementation of this notebook, you need to install `pyspark` and `findspark`

To work with PySpark, you will need to install both pyspark and findspark. However, the installation order depends on your specific setup and requirements.

1. If you already have Spark installed on your machine and want to use PySpark with it, you should install pyspark first. pyspark is the Python library that provides the Python API for Spark. You can install it using pip:

```cmd
pip install pyspark
```

2. After installing pyspark, you can install findspark. findspark is a Python library that helps you locate the Spark installation on your machine and makes it available in your Python environment. You can install it using pip as well.

```cmd
pip install findspark
```

Once you have both pyspark and findspark installed, you can import findspark in your Python script or notebook and use it to locate and initialize the Spark installation:

```cmd
import findspark
findspark.init()
```

This will ensure that you can import and use the pyspark library in your Python code.

Remember to also have Spark installed on your machine before using PySpark. You can download Spark from the official [Apache Spark website](https://spark.apache.org/downloads.html) and follow the installation instructions for your operating system.


