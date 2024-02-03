import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1706947892480 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1706947892480",
)

# Script generated for node customer_trusted
customer_trusted_node1706947875027 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1706947875027",
)

# Script generated for node Join
Join_node1706947964595 = Join.apply(
    frame1=accelerometer_trusted_node1706947892480,
    frame2=customer_trusted_node1706947875027,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706947964595",
)

# Script generated for node Drop Fields
DropFields_node1706948289325 = DropFields.apply(
    frame=Join_node1706947964595,
    paths=["z", "y", "x", "user", "timestamp"],
    transformation_ctx="DropFields_node1706948289325",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706951608247 = DynamicFrame.fromDF(
    DropFields_node1706948289325.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706951608247",
)

# Script generated for node customer_curated
customer_curated_node1706948391965 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1706951608247,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1706948391965",
)

job.commit()
