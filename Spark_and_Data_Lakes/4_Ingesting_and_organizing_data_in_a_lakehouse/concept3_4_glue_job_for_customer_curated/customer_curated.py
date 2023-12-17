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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1702811490994 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1702811490994",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702811494035 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1702811494035",
)

# Script generated for node Join
Join_node1702811563126 = Join.apply(
    frame1=CustomerTrusted_node1702811494035,
    frame2=AccelerometerLanding_node1702811490994,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1702811563126",
)

# Script generated for node Drop Fields
DropFields_node1702812340008 = DropFields.apply(
    frame=Join_node1702811563126,
    paths=["email", "phone"],
    transformation_ctx="DropFields_node1702812340008",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702838752494 = DynamicFrame.fromDF(
    DropFields_node1702812340008.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702838752494",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1702811624654 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1702838752494,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dana-data-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrustedZone_node1702811624654",
)

job.commit()
