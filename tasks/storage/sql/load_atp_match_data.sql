INSERT INTO {database}.{table_name}
WITH json_content AS (
    SELECT
        content::JSON  AS data,
        filename       AS meta_file_name
    FROM read_text('s3://{bucket}/{pattern}/*.json')
)
SELECT 
    data,
    meta_file_name,
    CAST(data->>'$.metadata.retrieved_at' AS TIMESTAMP) AS meta_file_modified
FROM json_content;
