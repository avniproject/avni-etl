select STRING_AGG('address."' || alt.name || '"', ', ')
from public.address_level_type alt
         join organisation o on alt.organisation_id = o.id
where
      not alt.is_voided
and o.db_user = '<db_user>';
