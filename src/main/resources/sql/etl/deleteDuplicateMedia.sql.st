DELETE
FROM "<schemaName>"."media" t2
WHERE t2.id IN (
    SELECT duplicates.dup_id
    FROM (SELECT t1.id AS dup_id, ROW_NUMBER() OVER (PARTITION BY uuid, entity_id) AS occurence
          FROM "<schemaName>"."media" t1) AS duplicates
    WHERE duplicates.occurence > 1
);