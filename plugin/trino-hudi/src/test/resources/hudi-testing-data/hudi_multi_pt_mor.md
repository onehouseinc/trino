## Create script
Revision: 6f65998117a2d1228fc96d36053bd0d394499afe

```scala
test("Create MOR table with multiple partition fields with multiple types") {
  withTempDir { tmp =>
    val tableName = "hudi_multi_pt_mor"
    // Save current session timezone and set to UTC for consistency in test
    val originalTimeZone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts LONG,
         |  -- Partition Fields --
         |  category STRING,            -- Partition field 1: STRING
         |  year INT,                   -- Partition field 2: INT
         |  event_date DATE,            -- Partition field 3: DATE
         |  batch_id BIGINT,            -- Partition field 4: BIGINT
         |  metric_value DECIMAL(10,2), -- Partition field 5: DECIMAL
         |  event_timestamp TIMESTAMP,  -- Partition field 6: TIMESTAMP
         |  is_feature_enabled BOOLEAN  -- Partition field 7: BOOLEAN
         |) USING hudi
         | LOCATION '${tmp.getCanonicalPath}'
         | TBLPROPERTIES (
         |  primaryKey = 'id,name',
         |  type = 'mor',
         |  preCombineField = 'ts'
         | )
         | PARTITIONED BY (category, year, event_date, batch_id, metric_value, event_timestamp, is_feature_enabled)
     """.stripMargin)

    // Configure Hudi properties
    spark.sql(s"SET hoodie.parquet.small.file.limit=0") // Write to a new parquet file for each commit
    spark.sql(s"SET hoodie.metadata.compact.max.delta.commits=1")
    spark.sql(s"SET hoodie.metadata.enable=true")
    spark.sql(s"SET hoodie.metadata.index.column.stats.enable=true")
    spark.sql(s"SET hoodie.compact.inline.max.delta.commits=9999") // Disable compaction plan trigger

    // Update column list for column stats index
    spark.sql(s"SET hoodie.metadata.index.column.stats.column.list=_hoodie_commit_time,_hoodie_partition_path,_hoodie_record_key,id,name,price,ts,category,year,event_date,batch_id,metric_value,event_timestamp,is_feature_enabled")
    // Insert data with new partition values
    spark.sql(s"INSERT INTO $tableName VALUES(1, 'a1', 100.0, 1000, 'books', 2023, date'2023-01-15', 10000000001L, decimal('123.45'), timestamp'2023-01-15 10:00:00.123', true)")
    spark.sql(s"INSERT INTO $tableName VALUES(2, 'a2', 200.0, 1000, 'electronics', 2023, date'2023-03-10', 10000000002L, decimal('50.20'), timestamp'2023-03-10 12:30:00.000', false)")
    spark.sql(s"INSERT INTO $tableName VALUES(3, 'a3', 101.0, 1001, 'books', 2024, date'2024-02-20', 10000000003L, decimal('75.00'), timestamp'2024-02-20 08:45:10.456', true)")
    spark.sql(s"INSERT INTO $tableName VALUES(4, 'a4', 201.0, 1001, 'electronics', 2023, date'2023-03-10', 10000000002L, decimal('50.20'), timestamp'2023-03-10 12:30:00.000', true)") // Same as record 2 part except boolean
    spark.sql(s"INSERT INTO $tableName VALUES(5, 'a5', 300.0, 1002, 'apparel', 2024, date'2024-01-05', 20000000001L, decimal('99.99'), timestamp'2024-01-05 18:00:00.789', false)")

    // Generate logs through updates
    spark.sql(s"UPDATE $tableName SET price = price + 2.0 WHERE is_feature_enabled = true AND category = 'books'")
    spark.sql(s"UPDATE $tableName SET price = ROUND(price * 1.02, 2) WHERE batch_id = 10000000002L")

    println("asd")
  }
}
```
