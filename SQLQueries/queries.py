import awswrangler as wr
import boto3
import pandas as pd

boto3.setup_default_session(profile_name="nateTest")

# Average Call Time within a date range in seconds
def getAverageCallTime(df, startDate, endDate):
    # Ensure that these are datetime formats so we can grab seconds
    df["callstarttime"] = pd.to_datetime(df["callstarttime"], infer_datetime_format=True)
    df["callendtime"] = pd.to_datetime(df["callendtime"], infer_datetime_format=True)

    # Filter out records that exist outside of these date ranges
    mask = (df['callstarttime'] >= startDate) & (df['callstarttime'] < endDate)
    df2 = df.loc[mask]

    # Calculate seconds of calls then average them out
    averageCallTime = df2["callendtime"]- df2["callstarttime"]
    averageCallTime = averageCallTime.dt.total_seconds().mean()
    return averageCallTime

# Get total number of calls an agent has per channel
def getTotalCalls(df):
    dfGroup = df.groupby(["agentid", "channel"])["contactid"].count()
    totalCalls = pd.DataFrame(dfGroup).sort_values(by=['agentid'], ascending=False)
    totalCalls.rename(columns={'contactid':'totalcalls'}, inplace=True)
    return totalCalls

# Connect to redshift DB
con = wr.redshift.connect("redshift")
# Get full dataframe from database for use.
df = wr.redshift.read_sql_query("SELECT * FROM craft.rs_call_logs", con=con)

# Can switch to use postgresSQL DB as well
# con = wr.postgresql.connect("RDS")
# df = wr.postgresql.read_sql_query("SELECT * FROM craft.rs_call_logs", con=con)

startDate = '2019-10-15'
endDate = '2019-10-31'

# Call function for average call time and print out result
averageCallTime = getAverageCallTime(df, startDate, endDate)
print (f'The Average call time in seconds for the input data range is: {averageCallTime}\n\n')

# PRINT OUT
# The Average call time in seconds for input data range is: 860.3214285714286

# Get total calls per Agent/Channel
totalCalls = getTotalCalls(df)
print ("Total Number of Calls served by each agent per channel")
print (totalCalls)

# PRINT OUT
# Total Number of Calls served by each agent per channel
# agentid          channel       totalcalls
# 9876u6tg-24b9    VOICE             1
# 9656ua8a-24b9    VOICE            10
#                  CHAT              7
# 7f47yu6tg-u7huj  VOICE             3
# 7f47yu6tg-78jhuj VOICE             2
#                  CHAT              2
# 78rf4u6tg-u7huj  VOICE             2
# 754ru6tg-24b9    CHAT              1
# 7546u6tg-7huj    VOICE             6
#                  CHAT              1
# 7546u6tg-24b9    VOICE             8
#                  CHAT              3
# 6k5cfa8a-24b9    CHAT              1
# 6h5cfa8a-24b9    CHAT              1
# 665cfa8a-24b9    VOICE            10
#                  CHAT              6
# 645cfa8a-24b9    VOICE             1
# 4h5cfa8a-24b9    CHAT              1
# 4gyua8a-24b9     VOICE            21
#                  CHAT              6
# 4gyra8a-24b9     VOICE             1
# 4gyia8a-24b9     CHAT              1
# 3hk5cfa8a-24b9   CHAT              1
# 32rf4u6tg-89huj  VOICE             4
#                  CHAT              1
# 3246u6tg-7huj    CHAT              3
#                  VOICE             5
# 2h5cfa8a-24b9    CHAT              1











