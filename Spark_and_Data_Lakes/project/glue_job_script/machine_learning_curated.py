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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1706976765119 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1706976765119",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1706976816895 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1706976816895",
)

# Script generated for node SQL Query
SqlQuery873 = """
select
    sensorreadingtime,
    serialnumber,
    user,
    x,
    y,
    z,
    distancefromobject
from
    myDataSource
join
    accelerometer
on
    myDataSource.sensorreadingtime = accelerometer.timestamp;
"""

SQLQuery_node1706976995505 = sparkSqlQuery(
    glueContext,
    query=SqlQuery873,
    mapping={
        "myDataSource": step_trainer_trusted_node1706976765119,
        "accelerometer": accelerometer_trusted_node1706976816895,
    },
    transformation_ctx="SQLQuery_node1706976995505",
)


# Script generated for node machine_learing_curated
machine_learing_curated_node1706977147234 = (
    glueContext.write_dynamic_frame.from_options(
        frame=SQLQuery_node1706976995505,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://stedi-data-lake-house/step_trainer/curated/",
            "partitionKeys": [],
        },
        transformation_ctx="machine_learing_curated_node1706977147234",
    )
)

job.commit()
