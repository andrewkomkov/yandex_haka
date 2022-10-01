truncate table team_1_dds.$directory;
select setval('$directory_id_seq', 1, False);