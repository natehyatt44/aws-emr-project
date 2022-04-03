from pyspark import SparkContext
from pyspark.sql import Row, Window
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, col
import boto3

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)

############################ Set Variables ##################################
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bucket",
        "table"
    ]
)
bucket = args["bucket"]
table = args["table"]
redshiftTempDir = f"s3://{bucket}/redshift/temp/"

# Dropzone Folder
dropzone = f's3://{bucket}/dropzone/{table}/'

############################ Load call log file data frame ##################################
try:
    print(f"Retrieve staging frame from {dropzone}")
    stgFrame = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [dropzone],
            "recurse": True
        }
    )

    print(f'Staging Frame Row Count: {stgFrame.count()}')

except Exception as e:
    raise e
    job.commit()

if stgFrame.count() == 0:
    print('Exiting job as call log data frame is empty')
    job.commit()

############################ Transform/Dedup Call Log Frame ##################################
try:
    print(f"Transform/Dedup call log frame")
    stgFrame = stgFrame.toDF()
    stgFrame.createOrReplaceTempView("stgFrame")

    # Filter out where channel is empty
    stgFrame = glueContext.sql("""
        SELECT  CAST(Contactid AS STRING) AS contactid,
                CAST(Agentid AS STRING) AS agentid, 
                CAST(Channel AS STRING) AS channel,
                CAST(InitiationMethod AS STRING) AS initiationmethod,
                CAST(NextContactId AS STRING) AS nextcontactid,
                CAST(InitialContactId AS STRING) AS initialcontactid,
                CAST(PreviousContactId AS STRING) AS previouscontactid,
                CallStartTimestamp AS callstarttime,
                CallEndTimestamp AS callendtime,
                attributes AS attributes
        FROM    stgframe 
        WHERE   channel IS NOT NULL
        AND     channel <> ''
    """)

    # Drop Duplicates on contactId (Unique ID for each call/contact)
    # Include callStartTime for duplicates as well as there are instances of the same contactId with different startTimes to the call
    stgFrame = stgFrame.dropDuplicates(["contactid", "callstarttime"])
    # Convert attributes to a json string so it can inserted into the postgres DB
    stgFrame = stgFrame.withColumn("attributes", to_json(col("attributes")))

    print(f'Staging Frame Row Count after Dedup/Transform: {stgFrame.count()}')

except Exception as e:
    raise e
    job.commit()

############################# Get frame from postgres DB ##################################
try:
    print(f"Get frame from postgres DB")
    dbFrame = glueContext.create_dynamic_frame.from_catalog(
        database="craftproject",
        table_name="craftproject_craft_call_logs",
        redshift_tmp_dir=redshiftTempDir)

    print(f'DB Frame Count: {dbFrame.count()}')
except Exception as e:
    raise e
    job.commit()

############################ Merge call log incr frame to db frame ##################################
try:
    print(f'Merge call log incr / db Frames')
    dbFrame = dbFrame.toDF()
    dbFrame = glueContext.createDataFrame(dbFrame.rdd, stgFrame.schema)

    joinConditions = [dbFrame.contactid == stgFrame.contactid,
                      dbFrame.callstarttime == stgFrame.callstarttime]

    finalFrame = stgFrame.join(dbFrame, joinConditions, 'left') \
        .select(stgFrame["*"]) \
        .where(dbFrame.contactid.isNull())

    print(f'Merged Frame Count: {finalFrame.count()}')

except Exception as e:
    raise e
    job.commit()

try:
    # If data frame is empty the iterator won't work (could use a better solution here)
    recordsLoaded = finalFrame.count()
except Exception as e:
    recordsLoaded = 0
    job.commit()

############################ Upload incremental data to postgres DB ##################################
try:
    if recordsLoaded > 0:
        print("Incremental load into postgres DB")
        postgresFrame = DynamicFrame.fromDF(finalFrame, glueContext, "finalFrame")

        glueContext.write_dynamic_frame.from_catalog(
            frame=postgresFrame,
            database="craftproject",
            table_name="craftproject_craft_call_logs",
            redshift_tmp_dir=redshiftTempDir)
    else:
        print(f"No records loaded")

    job.commit()

except Exception as e:
    raise e
    job.commit()

job.commit()


