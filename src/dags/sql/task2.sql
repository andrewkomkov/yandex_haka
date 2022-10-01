DROP TABLE team_1_mart.task3;
CREATE TABLE IF NOT EXISTS team_1_mart.task3 (
                          page_url varchar(255) NOT NULL,
                          count int NOT null);
                         
INSERT INTO team_1_mart.task3(page_url,count)
WITH cte AS (
SELECT 
page_url_id,
lead(page_url_path_id) OVER (ORDER BY event_timestamp) AS next_page
FROM team_1_dds.facts_1
ORDER BY event_timestamp)
SELECT 
pu.page_url,
count(*)
FROM cte 
LEFT JOIN team_1_dds.page_url pu ON pu.id = cte.page_url_id
WHERE next_page = 22
GROUP BY 1
ORDER BY 2 DESC 
LIMIT 10;
