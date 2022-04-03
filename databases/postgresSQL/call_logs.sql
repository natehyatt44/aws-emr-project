CREATE TABLE IF NOT EXISTS craft.call_logs
(
    contactid text COLLATE pg_catalog."default",
    agentid text COLLATE pg_catalog."default",
    channel text COLLATE pg_catalog."default",
    initiationmethod text COLLATE pg_catalog."default",
    nextcontactid text COLLATE pg_catalog."default",
    initialcontactid text COLLATE pg_catalog."default",
    previouscontactid text COLLATE pg_catalog."default",
    callstarttime text COLLATE pg_catalog."default",
    callendtime text COLLATE pg_catalog."default",
    attributes text COLLATE pg_catalog."default"
)

TRUNCATE TABLE craft.call_logs
SELECT * FROM craft.call_logs

SELECT  AVG(EXTRACT(EPOCH FROM (cast(callendtime as timestamp) - cast(callstarttime as timestamp)))) AS averageCallTime
FROM 	craft.call_logs
WHERE   to_date(callstarttime, 'YYYY-MM-DD') >= '2019-10-15'
AND 	to_date(callstarttime, 'YYYY-MM-DD') <= '2019-10-30'

SELECT 	agentid, channel, count(contactid)
FROM 	craft.call_logs
GROUP BY agentid, channel
ORDER BY agentid desc