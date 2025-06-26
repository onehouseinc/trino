## Create script

Structure of table:
- MOR table with MDT enabled
- Revision: 6f65998117a2d1228fc96d36053bd0d394499afe

```scala
test("Create table with expression index partitioned mor") {
    withTempDir { tmp =>
        val tableName = "hudi_ei_pt_v8_mor"
        spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  col_date date,
               |  col_string string,
               |  col_bigint long,
               |  col_timestamp timestamp,
               |  ts long,
               |  country string
               |) using hudi
               | location '${tmp.getCanonicalPath}'
               | tblproperties (
               |  primaryKey ='id',
               |  type = 'mor',
               |  preCombineField = 'ts'
               | ) partitioned by (country)
               |""".stripMargin)
        // directly write to new parquet file
        spark.sql(s"set hoodie.parquet.small.file.limit=0")
        spark.sql(s"set hoodie.metadata.compact.max.delta.commits=1")

        // 2 filegroups per partition
        spark.sql(
            s"""
               | INSERT INTO $tableName
               | VALUES
               |  (1, date'2023-01-15', 'apple_pie', 1673740800000, timestamp '2023-01-15 10:00:00', 1000, 'SG')
               |""".stripMargin)

        spark.sql(
            s"""
               | INSERT INTO $tableName
               | VALUES
               |  (2, date'2023-02-20', 'banana_bread', 1676851200000, timestamp '2023-02-20 12:30:00', 1001, 'SG')
               |""".stripMargin)

        spark.sql(
            s"""
               | INSERT INTO $tableName
               | VALUES
               |  (3, date'2024-03-10', 'cherry_tart', 1709990400000, timestamp '2024-03-10 14:00:00', 1002, 'US')
               |""".stripMargin)

        spark.sql(
            s"""
               | INSERT INTO $tableName
               | VALUES
               |  (5, date'2024-04-05', 'date_fudge', 1712236800000, timestamp '2024-04-05 16:45:00', 1003, 'US')
               |""".stripMargin)

        // Create expression index
        // day
        spark.sql(
            s"""
               | CREATE INDEX IF NOT EXISTS col_date_day_idx ON $tableName
               | USING column_stats(col_date)
               | OPTIONS(expr='day')
               |""".stripMargin)

        // Create expression index
        // month
        spark.sql(
            s"""
               | CREATE INDEX IF NOT EXISTS col_date_month_idx ON $tableName
               | USING column_stats(col_date)
               | OPTIONS(expr='month')
               |""".stripMargin)

        // hour
        spark.sql(
            s"""
               | CREATE INDEX IF NOT EXISTS col_timestamp_hour_idx ON $tableName
               | USING column_stats(col_timestamp)
               | OPTIONS(expr='hour')
               |""".stripMargin)

        // from_unixtime
        spark.sql(
            s"""
               | CREATE INDEX IF NOT EXISTS col_bigint_datestr_idx ON $tableName
               | USING column_stats(col_bigint)
               | OPTIONS(expr='from_unixtime', format='yyyy-MM-dd')
               |""".stripMargin)

        // substring
        spark.sql(
            s"""
               | CREATE INDEX IF NOT EXISTS col_string_substr_idx ON $tableName
               | USING column_stats(col_string)
               | OPTIONS(expr='substring', pos='1', len='5')
               |""".stripMargin)

        // generate logs through updates
        spark.sql(s"UPDATE $tableName SET col_bigint = col_bigint + 8")

        spark.sql(s"SELECT * FROM $tableName WHERE day(col_date) = 15").show(false)
    }
}
```

# When to use this table?
- For test cases that require multiple filegroups in a partition
- For test cases that require filegroups that have a log file
- For test cases that require expression index
