CREATE TABLE "craft"."rs_call_logs"
(
    contactid character varying(256) encode lzo,
    agentid character varying(256) encode lzo,
    channel character varying(256) encode lzo,
    initiationmethod character varying(256) encode lzo,
    nextcontactid character varying(256) encode lzo,
    initialcontactid character varying(256) encode lzo,
    previouscontactid character varying(256) encode lzo,
    callstarttime character varying(256) encode lzo,
    callendtime character varying(256) encode lzo,
    attributes character varying(5000) encode lzo
);

TRUNCATE TABLE craft.rs_call_logs
SELECT * FROM craft.rs_call_logs

SELECT 	AVG(EXTRACT(EPOCH FROM (cast(callendtime as timestamp) - cast(callstarttime as timestamp)))) AS averageCallTime
FROM 	craft.rs_call_logs
WHERE 	to_date(callstarttime, 'YYYY-MM-DD') >= '2019-10-15'
AND 	to_date(callstarttime, 'YYYY-MM-DD') <= '2019-10-30'

SELECT 	agentid, channel, count(contactid)
FROM 	craft.rs_call_logs
GROUP BY agentid, channel
ORDER BY agentid desc

