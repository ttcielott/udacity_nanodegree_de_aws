import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1706947892480 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1706947892480",
)

# Script generated for node customer_landing
customer_landing_node1706947875027 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_landing_node1706947875027",
)

# Script generated for node Join
Join_node1706947964595 = Join.apply(
    frame1=accelerometer_landing_node1706947892480,
    frame2=customer_landing_node1706947875027,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706947964595",
)

# Script generated for node Drop Fields
DropFields_node1706948289325 = DropFields.apply(
    frame=Join_node1706947964595,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "phone",
        "email",
    ],
    transformation_ctx="DropFields_node1706948289325",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1706948391965 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706948289325,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node1706948391965",
)

job.commit()
