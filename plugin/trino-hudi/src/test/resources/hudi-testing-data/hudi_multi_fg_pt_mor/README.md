## Create script

Structure of table:
- MOR table with MDT enabled
- Revision: eb212c9dca876824b6c570665951777a772bc463
- Written with master branch 
- 2 Partitions [US, SG]
- 2 Filegroups per partition

```scala
test("Create table multi filegroup partitioned mor") {
    withTempDir { tmp =>
        val tableName = "hudi_multi_fg_pt_mor"
        spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  country string
               |) using hudi
               | location '${tmp.getCanonicalPath}'
               | tblproperties (
               |  primaryKey ='id',
               |  type = 'mor',
               |  preCombineField = 'ts'
               | ) partitioned by (country)
       """.stripMargin)
        // directly write to new parquet file
        spark.sql(s"set hoodie.parquet.small.file.limit=0")
        spark.sql(s"set hoodie.metadata.compact.max.delta.commits=1")
        spark.sql(s"set hoodie.metadata.index.column.stats.enable=true")
        spark.sql(s"set hoodie.metadata.index.column.stats.column.list=_hoodie_commit_time,_hoodie_partition_path,_hoodie_record_key,id,name,price,ts,country")
        // 2 filegroups per partition
        spark.sql(s"insert into $tableName values(1, 'a1', 11, 1000, 'SG'),(2, 'a2', 12, 1000, 'US')")
        spark.sql(s"insert into $tableName values(3, 'a3', 11, 1001, 'SG'),(4, 'a3', 12, 1001, 'US')")
        // generate logs through updates
        spark.sql(s"update $tableName set price=price+1")
    }
}
```

# When to use this table?
- For test cases that require multiple filegroups in a partition
- For test cases that require filegroups that have a log file
- For test cases that require column stats index
- For test cases that require partition stats index
