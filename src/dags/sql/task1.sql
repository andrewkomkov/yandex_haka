DROP TABLE team_1_mart.task1;

CREATE TABLE IF NOT EXISTS team_1_mart.task1 (
                          count int NOT NULL,
                          hour int NOT null);
                         
INSERT INTO team_1_mart.task1(count,hour)
SELECT count(x.*), 
extract('hour' from x.event_timestamp) AS hour
FROM team_1_dds.facts_1 x
GROUP BY 2;


DROP TABLE team_1_mart.task2;

CREATE TABLE IF NOT EXISTS team_1_mart.task2 (
                          count int NOT NULL,
                          hour int NOT null);
                         
INSERT INTO team_1_mart.task2(count,hour)
SELECT count(x.*), 
extract('hour' from x.event_timestamp) AS hour
FROM team_1_dds.facts_1 x
WHERE page_url_path_id = 22
GROUP BY 2;
