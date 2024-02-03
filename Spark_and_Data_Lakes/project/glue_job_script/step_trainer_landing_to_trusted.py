import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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

# Script generated for node step_trainer_landing
step_trainer_landing_node1706973051891 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1706973051891",
)

# Script generated for node customer_curated
customer_curated_node1706973135658 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1706973135658",
)

# Script generated for node SQL Query
SqlQuery874 = """
select * from myDataSource m
join customer_curated c on m.serialNumber = c.serialNumber;
"""
SQLQuery_node1706973204945 = sparkSqlQuery(
    glueContext,
    query=SqlQuery874,
    mapping={
        "myDataSource": step_trainer_landing_node1706973051891,
        "customer_curated": customer_curated_node1706973135658,
    },
    transformation_ctx="SQLQuery_node1706973204945",
)

# Script generated for node Drop Fields
DropFields_node1706973404148 = DropFields.apply(
    frame=SQLQuery_node1706973204945,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1706973404148",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1706973448258 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706973404148,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1706973448258",
)

job.commit()
