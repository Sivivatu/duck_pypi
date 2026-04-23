-- Test query to list all schemas in the sample_data database
SELECT schema_name 
FROM information_schema.schemata 
ORDER BY schema_name
