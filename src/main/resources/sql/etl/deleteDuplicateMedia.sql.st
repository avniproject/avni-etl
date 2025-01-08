delete
from "<schemaName>"."media"
where uuid in (
    select t1.uuid
    from "<schemaName>"."media" t1
             inner join "<schemaName>"."media" t2 on
                t1.uuid = t2.uuid
            and t1.last_modified_date_time \<= :lastSyncTime
            and t2.last_modified_date_time > :lastSyncTime
            and t1.last_modified_date_time \< t2.last_modified_date_time)
  and last_modified_date_time \<= :lastSyncTime;
