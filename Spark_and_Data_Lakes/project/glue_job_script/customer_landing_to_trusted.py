import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import re


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing
customer_landing_node1706745178816 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node1706745178816",
)

# Script generated for node SQL Query
SqlQuery912 = """
select * from myDataSource
WHERE sharewithresearchasofdate is not null;

"""
SQLQuery_node1706945117339 = sparkSqlQuery(
    glueContext,
    query=SqlQuery912,
    mapping={"myDataSource": customer_landing_node1706745178816},
    transformation_ctx="SQLQuery_node1706945117339",
)

# Script generated for node Filter Opt-in
FilterOptin_node1706745327030 = Filter.apply(
    frame=SQLQuery_node1706945117339,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="FilterOptin_node1706745327030",
)

# Script generated for node Amazon S3
AmazonS3_node1706745509957 = glueContext.write_dynamic_frame.from_options(
    frame=FilterOptin_node1706745327030,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-house/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1706745509957",
)

job.commit()
