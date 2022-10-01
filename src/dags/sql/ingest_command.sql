INSERT INTO team_1_dds.$directory($directory)
SELECT 
    DISTINCT object_value::json->>$directory AS $directory
    now(),
    '2099-01-01'
FROM team_1_stg.logs
ON CONFLICT DO NOTHING;