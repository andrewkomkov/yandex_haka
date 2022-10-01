INSERT INTO team_1_dds.facts_1
(event_id, event_timestamp, event_type_id, page_url_path_id, ip_address_id,page_url_id)
WITH cte AS (
SELECT object_value::json->>'event_id' AS event_id
,object_value::json->>'event_timestamp' AS event_timestamp
,object_value::json->>'event_type' AS event_type
,object_value::json->>'page_url_path' AS page_url_path
,object_value::json->>'ip_address' AS ip_address
,object_value::json->>'page_url' AS page_url
FROM team_1_stg.logs)
SELECT
team_1_dds.event_id.id AS event_id,
c.event_timestamp :: timestamp,
team_1_dds.event_type.id AS event_type_id,
team_1_dds.page_url_path.id AS page_url_path_id,
team_1_dds.ip_address.id AS ip_address_id,
team_1_dds.page_url.id AS page_url_id
FROM cte c
LEFT JOIN team_1_dds.event_id ON c.event_id = team_1_dds.event_id.event_id
LEFT JOIN team_1_dds.event_timestamp ON c.event_timestamp = team_1_dds.event_timestamp.event_timestamp
LEFT JOIN team_1_dds.event_type ON c.event_type = team_1_dds.event_type.event_type
LEFT JOIN team_1_dds.page_url_path ON c.page_url_path = team_1_dds.page_url_path.page_url_path
LEFT JOIN team_1_dds.ip_address ON c.ip_address = team_1_dds.ip_address.ip_address
LEFT JOIN team_1_dds.page_url ON c.page_url = team_1_dds.page_url.page_url;