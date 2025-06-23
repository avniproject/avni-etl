package org.avniproject.etl.repository.sql;

import org.avniproject.etl.builder.OrganisationIdentityBuilder;
import org.avniproject.etl.builder.domain.metadata.TableMetadataBuilder;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Calendar;

import static java.util.Calendar.MILLISECOND;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class SqlGeneratorTest {

    private ColumnMetadata createColumnMetaData(Integer id, String name, Integer conceptId, ColumnMetadata.ConceptType conceptType, String conceptUuid) {
        return new ColumnMetadata(id, conceptType == null ? new Column(name, null) : new Column(name, conceptType.getColumnDatatype()), conceptId, conceptType, conceptUuid, null, null, false);
    }

    @Test
    public void shouldGenerateSql() throws IOException {
        OrgIdentityContextHolder.setContext(new OrganisationIdentityBuilder().build());
        TableMetadata tableMetadata = new TableMetadataBuilder()
                .forIndividual()
                .withColumnMetadata(createColumnMetaData(1, "numeric field", 10, ColumnMetadata.ConceptType.Numeric, "uuid-1"))
                .withColumnMetadata(createColumnMetaData(1, "text field", 10, ColumnMetadata.ConceptType.Text, "uuid-2"))
                .withColumnMetadata(createColumnMetaData(1, "single select", 10, ColumnMetadata.ConceptType.SingleSelect, "uuid-3"))
                .withColumnMetadata(createColumnMetaData(1, "multi select", 10, ColumnMetadata.ConceptType.MultiSelect, "uuid-4"))
                .withColumnMetadata(createColumnMetaData(1, "date", 10, ColumnMetadata.ConceptType.Date, "uuid-5"))
                .build();
        Calendar startTime = Calendar.getInstance();
        startTime.set(1970, 1, 12, 13, 23, 4);
        startTime.set(MILLISECOND, 0);

        Calendar endTime = Calendar.getInstance();
        endTime.set(2021, 10, 11, 13, 23, 4);
        endTime.set(MILLISECOND, 0);

        String output = "--[Data Extract Report] Registration\n" +
                "insert into \"schema\".\"Individual\" (\n" +
                "    \"id\", \"address_id\", \"uuid\", \"first_name\", \"last_name\", \"registration_date\", \"registration_location\",\n" +
                "    \"is_voided\", \"created_by_id\", \"last_modified_by_id\", \"created_date_time\",\n" +
                "    \"last_modified_date_time\", \"organisation_id\", \"legacy_id\"\n" +
                "        , \"numeric field\", \"text field\", \"single select\", \"multi select\", \"date\"\n" +
                ")\n" +
                "    (with Individual_concept_maps as (SELECT public.hstore((array_agg(c2.uuid)) :: text [], (array_agg(c2.name)) :: text []) AS map\n" +
                "                  FROM public.concept\n" +
                "                         join public.concept_answer a on concept.id = a.concept_id\n" +
                "                         join public.concept c2 on a.answer_concept_id = c2.id\n" +
                "                  where concept.uuid in ('dummy', 'uuid-1', 'uuid-2', 'uuid-3', 'uuid-4', 'uuid-5'))\n" +
                "        SELECT entity.id                                                                as \"id\",\n" +
                "        entity.address_id                                                               as \"address_id\",\n" +
                "        entity.uuid                                                                     as \"uuid\",\n" +
                "        entity.first_name                                                               as \"first_name\",\n" +
                "        entity.last_name                                                                as \"last_name\",\n" +
                "        entity.registration_date                                                        as \"registration_date\",\n" +
                "        entity.registration_location                                                    as \"registration_location\",\n" +
                "        entity.is_voided                                                                as \"is_voided\",\n" +
                "        entity.created_by_id                                                            as \"created_by_id\",\n" +
                "        entity.last_modified_by_id                                                      as \"last_modified_by_id\",\n" +
                "        entity.created_date_time                                                        as \"created_date_time\",\n" +
                "        entity.last_modified_date_time                                                  as \"last_modified_date_time\",\n" +
                "        entity.organisation_id                                                          as \"organisation_id\",\n" +
                "        entity.legacy_id                                                                as \"legacy_id\"\n" +
                "        ,(entity.observations->> 'uuid-1')::NUMERIC as \"numeric field\",\n" +
                "(entity.observations->> 'uuid-2')::TEXT as \"text field\",\n" +
                "public.get_coded_string_value(entity.observations-> 'uuid-3', Individual_concept_maps.map)::TEXT as \"single select\",\n" +
                "public.get_coded_string_value(entity.observations-> 'uuid-4', Individual_concept_maps.map)::TEXT as \"multi select\",\n" +
                "((entity.observations->> 'uuid-5')::timestamptz AT time zone 'asia/kolkata')::date as \"date\"\n" +
                "        FROM public.individual entity\n" +
                "        LEFT OUTER JOIN public.subject_type st on st.id = entity.subject_type_id\n" +
                "        cross join Individual_concept_maps\n" +
                "        LEFT OUTER JOIN public.gender g ON g.id = entity.gender_id\n" +
                "        LEFT OUTER JOIN public.address_level a ON entity.address_id = a.id\n" +
                "        where st.uuid = '1'\n" +
                "        and entity.last_modified_date_time > '1970-02-12T13:23:04.000'\n" +
                "        and entity.last_modified_date_time <= '2021-11-11T13:23:04.000')";

        System.out.println(new TransactionalSyncSqlGenerator().generateSql(tableMetadata, startTime.getTime(), endTime.getTime()));
        assertThat(new TransactionalSyncSqlGenerator().generateSql(tableMetadata, startTime.getTime(), endTime.getTime()), equalTo(output));
    }

}
