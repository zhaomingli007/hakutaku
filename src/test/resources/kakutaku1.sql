with base as (
  select distinct profile_id,
  get_json_object(properties, '$.pageUrl')          as pageUrl,
  get_json_object(properties, '$.pageIsFirstEntry') as pageIsFirstEntry
  from _SAMPLE_DATA_
  where name = 'pageview'
  )
select profile_id, pageIsFirstEntry,collect_list(pageUrl) as page_set from base group by profile_id,pageIsFirstEntry