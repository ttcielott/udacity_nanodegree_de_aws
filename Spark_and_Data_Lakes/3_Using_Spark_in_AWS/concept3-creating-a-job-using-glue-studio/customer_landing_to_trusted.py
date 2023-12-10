import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1702154918119 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dana-data-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1702154918119",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1702154974146 = Filter.apply(
    frame=CustomerLanding_node1702154918119,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="PrivacyFilter_node1702154974146",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702155282257 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1702154974146,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dana-data-lakehouse/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1702155282257",
)

job.commit()
