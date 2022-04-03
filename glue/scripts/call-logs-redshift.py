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
        "JOB_NAME"
    ]
)

redshiftTempDir = f"s3://craft-project/redshift/temp/"

############################ Load call log file data frame from postgres DB ##################################
try:
    print(f"Get frame from postgres DB")
    dbFrame = glueContext.create_dynamic_frame.from_catalog(
        database="craftproject",
        table_name="craftproject_craft_call_logs",
        redshift_tmp_dir=redshiftTempDir)

    print(f'DB Frame Count: {dbFrame.count()}')
    dbFrame = dbFrame.toDF()
except Exception as e:
    raise e
    job.commit()

if dbFrame.count() == 0:
    print('Exiting job as call log data frame is empty')
    job.commit()

############################# Get frame from redshift DB ##################################
try:
    print(f"Get frame from postgres DB")
    rsFrame = glueContext.create_dynamic_frame.from_catalog(
        database="craftproject",
        table_name="craftproject_craft_rs_call_logs",
        redshift_tmp_dir=redshiftTempDir)

    print(f'Redshift Frame Count: {rsFrame.count()}')
    rsFrame = rsFrame.toDF()  # convert to spark frame
except Exception as e:
    raise e
    job.commit()

# If there is existing data in redshift merge the frames.
if (len(rsFrame.head(1))) > 0:
    ############################ Merge call log incr frame to redshift frame ##################################
    try:
        print(f'Merge call log incr / db Frames')
        rsFrame = glueContext.createDataFrame(rsFrame.rdd, dbFrame.schema)
        joinConditions = [rsFrame.contactid == dbFrame.contactid,
                          rsFrame.callstarttime == dbFrame.callstarttime]

        finalFrame = dbFrame.join(rsFrame, joinConditions, 'left') \
            .select(dbFrame["*"]) \
            .where(rsFrame.contactid.isNull())

        print(f'Merged Frame Count: {finalFrame.count()}')

    except Exception as e:
        raise e
        job.commit()

############################ Upload incremental data to redshift DB ##################################
try:
    print("Incremental load into redshift DB")
    if (len(rsFrame.head(1))) > 0:
        redshiftFrame = DynamicFrame.fromDF(finalFrame, glueContext, "finalFrame")
    else:
        redshiftFrame = DynamicFrame.fromDF(dbFrame, glueContext, "dbFrame")

    glueContext.write_dynamic_frame.from_catalog(
        frame=redshiftFrame,
        database="craftproject",
        table_name="craftproject_craft_rs_call_logs",
        redshift_tmp_dir=redshiftTempDir)

except Exception as e:
    raise e
    job.commit()

job.commit()

