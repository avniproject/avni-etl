DROP VIEW IF EXISTS "<schema_name>"."<view_name>";
CREATE OR REPLACE VIEW "<schema_name>"."<view_name>" AS
<columns:{col |
    SELECT
        t.id,
        t.is_voided,
        t.last_modified_date_time,
        '<col.conceptId>' as concept_id,
        '<col.conceptUuid>' as concept_uuid,
        '<col.name>' as concept_name,
        '<col.parentConceptUuid>' as parent_concept_uuid,
        '<col.parentConceptName>' as parent_concept_name,
        string_to_table(t."<col.name>", ', ') as answer
    FROM "<schema_name>"."<table_name>" t
    WHERE t.is_voided = false
}; separator="UNION ALL\n">
