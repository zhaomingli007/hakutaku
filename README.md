## Data processing and save to redis


### Design and implementation
Using spark SQL handling all ETL logics.  
Modeling the business logic as Source,Target,Connector, Job, Reader and Writer.  
Parameterize the ETL jobs.   

### Project structure

![](document/proj.png)

### Test data
Configurations are now specified in [Application](src/main/scala/com/hakutaku1/service/Application.scala) class, from where the test data and corresponding SQL files are 
set.  
The two SQLs are stored at [kakutaku1.sql](src/test/resources/kakutaku1.sql) and [kakutaku2.sql](src/test/resources/kakutaku2.sql)  

- kakutaku1.sql
```sql
with base as (
  select distinct profile_id,
  get_json_object(properties, '$.pageUrl')          as pageUrl,
  get_json_object(properties, '$.pageIsFirstEntry') as pageIsFirstEntry
  from _SAMPLE_DATA_
  where name = 'pageview'
  )
select profile_id, pageIsFirstEntry,collect_list(pageUrl) as page_set from base group by profile_id,pageIsFirstEntry
```
- kakutaku2.sql
```sql
select profile_id || '_' || pageIsFirstEntry as profIsFirst,size(page_set) as size from _SAMPLE_DATA_2
```

### Run
`sbt "runMain com.hakutaku1.service.Application"`  

Screenshot of out1:
![](document/out1.png)

### Future work
Make all the configurations as external file(json or yaml)