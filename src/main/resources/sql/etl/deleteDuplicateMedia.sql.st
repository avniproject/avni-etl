DELETE
FROM "<schemaName>"."media" t2
WHERE t2.id IN (
    SELECT duplicates.dup_id
    FROM (SELECT t1.id AS dup_id, RANK() OVER (PARTITION BY entity_id ORDER BY last_modified_date_time DESC) AS rank
          FROM "<schemaName>"."media" t1) AS duplicates
    WHERE duplicates.rank > 1
);