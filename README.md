# 🐍 📄 PySpark Cheat Sheet

A quick reference guide to the most commonly used patterns and functions in PySpark SQL.

#### Table of Contents

- [Common Patterns](#common-patterns)
    - [Create session](#create-session)
    - [Data Source API](#data-source)
    - [Importing Functions & Types](#importing-functions--types)
    - [Filtering](#filtering)
    - [Joins](#joins)
    - [Creating New Columns](#creating-new-columns)
    - [Coalescing Values](#coalescing-values)
    - [Casting, Nulls & Duplicates](#casting-nulls--duplicates)
- [Column Operations](#column-operations)
- [Number Operations](#number-operations)
- [String Operations](#string-operations)
    - [String Filters](#string-filters)
    - [String Functions](#string-functions)
- [Date Operations](#date-operations)
- [Array Operations](#array-operations)
- [Aggregation Operations](#aggregation-operations)
- [Advanced Operations](#advanced-operations)
    - [Repartitioning](#repartitioning)
    - [UDFs (User Defined Functions)](#udfs-user-defined-functions)
- [SQL](#sql)
- [Stream](#stream)

If you can't find what you're looking for, check out the [PySpark Official Documentation](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and add it here!
 
## Common Patterns

#### Create session

```python
spark = SparkSession.builder.appName("pyspark").getOrCreate()
```

#### Data Source API

```python
# read
file_path = SPARK_BOOK_DATA_PATH + "/data/flight-data/csv/2010-summary.csv"
csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load(file_path)

file_path = SPARK_BOOK_DATA_PATH + "/data/flight-data/parquet/2010-summary.parquet"
csvFile = spark.read.format("parquet").load(file_path)

# write
csvFile.write.format("csv").mode("overwrite").option("sep", "\t")\
  .save("/tmp/my-tsv-file.tsv")
csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")

# sqlite database
file_path = SPARK_BOOK_DATA_PATH + "/data/flight-data/jdbc/my-sqlite.db"
driver = "org.sqlite.JDBC"
path = file_path
url = "jdbc:sqlite:" + path
tablename = "flight_info"
dbDataFrame = spark.read.format("jdbc").option("url", url).option("dbtable", tablename)\
    .option("driver",  driver).load()

# postgresql database
pgDF = spark.read.format("jdbc")\
  .option("driver", "org.postgresql.Driver")\
  .option("url", "jdbc:postgresql://database_server")\
  .option("dbtable", "schema.tablename")\
  .option("user", "username").option("password", "my-secret-password").load()
```


#### Importing Functions & Types

```python
# Easily reference these as F.my_function() and T.my_type() below
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
```

#### Filtering

```python
# Filter on equals condition
df = df.filter(df.is_adult == 'Y')

# Filter on >, <, >=, <= condition
df = df.filter(df.age > 25)

# Multiple conditions require parens around each
df = df.filter((df.age > 25) & (df.is_adult == 'Y'))
```

#### Joins

```python
# Left join in another dataset
df = df.join(person_lookup_table, 'person_id', 'left')

# Useful for one-liner lookup code joins if you have a bunch
def lookup_and_replace(df1, df2, df1_key, df2_key, df2_value):
    return (
        df1
        .join(df2[[df2_key, df2_value]], df1[df1_key] == df2[df2_key], 'left')
        .withColumn(df1_key, F.coalesce(F.col(df2_value), F.col(df1_key)))
        .drop(df2_key)
        .drop(df2_value)
    )

df = lookup_and_replace(people, pay_codes, id, pay_code_id, pay_code_desc)
```

#### Creating New Columns

```python
# Add a new static column
df = df.withColumn('status', F.lit('PASS'))

# Construct a new dynamic column
df = df.withColumn('full_name', F.when(
    (df.fname.isNotNull() & df.lname.isNotNull()), F.concat(df.fname, df.lname)
).otherwise(F.lit('N/A'))
```

#### Coalescing Values

```python
# Take the first value that is not null
df = df.withColumn('last_name', F.coalesce(df.last_name, df.surname, F.lit('N/A')))
```

#### Casting, Nulls & Duplicates

```python
# Cast a column to a different type
df = df.withColumn('price', df.price.cast(T.DoubleType()))

# Replace all nulls with a specific value
df = df.fillna({
    'first_name': 'Tom',
    'age': 0,
})

# Drop duplicate rows in a dataset (distinct)
df = df.dropDuplicates()

# Drop duplicate rows, but consider only specific columns
df = df.dropDuplicates(['name', 'height'])
```

## Column Operations

```python
# Pick which columns to keep, optionally rename some
df = df.select(
    'name',
    'age',
    F.col('dob').alias('date_of_birth'),
)

# Remove columns
df = df.drop('mod_dt', 'mod_username')

# Rename a column
df = df.withColumnRenamed('dob', 'date_of_birth')

# Keep all the columns which also occur in another dataset
df = df.select(*(F.col(c) for c in df2.columns))

# Batch Rename/Clean Columns
for col in df.columns:
    df = df.withColumnRenamed(col, col.lower().replace(' ', '_').replace('-', '_'))
```

## Number Operations

```python
# Round - F.round(col, scale=0)
df = df.withColumn('price', F.round('price', 0))

# Floor - F.floor(col)
df = df.withColumn('price', F.floor('price'))

# Ceiling - F.ceil(col)
df = df.withColumn('price', F.ceil('price'))
```


## String Operations

#### String Filters

```python
# Is Null - col.isNull()
df = df.filter(df.is_adult.isNull())

# Is Not Null - col.isNotNull()
df = df.filter(df.first_name.isNotNull())

# Contains - col.contains(string)
df = df.filter(df.name.contains('o'))

# Starts With - col.startswith(string)
df = df.filter(df.name.startswith('Al'))

# Ends With - col.endswith(string)
df = df.filter(df.name.endswith('ice'))

# Like - col.like(string_with_sql_wildcards)
df = df.filter(df.name.like('Al%'))

# Regex Like - col.rlike(regex)
df = df.filter(df.name.rlike('[A-Z]*ice$'))

# Is In List - col.isin(*cols)
df = df.filter(df.name.isin('Bob', 'Mike'))
```

#### String Functions

```python
# Substring - col.substr(startPos, length)
df = df.withColumn('short_id', df.id.substr(0, 10))

# Trim - F.trim(col)
df = df.withColumn('name', F.trim(df.name))

# Left Pad - F.lpad(col, len, pad)
# Right Pad - F.rpad(col, len, pad)
df = df.withColumn('id', F.lpad('id', 4, '0'))

# Left Trim - F.ltrim(col)
# Right Trim - F.rtrim(col)
df = df.withColumn('id', F.ltrim('id'))

# Concatenate - F.concat(*cols)
df = df.withColumn('full_name', F.concat('fname', F.lit(' '), 'lname'))

# Concatenate with Separator/Delimiter - F.concat_ws(*cols)
df = df.withColumn('full_name', F.concat_ws('-', 'fname', 'lname'))

# Regex Replace - F.regexp_replace(str, pattern, replacement)[source]
df = df.withColumn('id', F.regexp_replace(id, '0F1(.*)', '1F1-$1'))

# Regex Extract - F.regexp_extract(str, pattern, idx)
df = df.withColumn('id', F.regexp_extract(id, '[0-9]*', 0))
```



## Date Operations

```python
dateDF = spark.range(10)\
  .withColumn("today", F.current_date())\
  .withColumn("now", F.current_timestamp())

# date_add, date_sub
dateDF.select(F.date_sub(F.col("today"), 5), F.date_add(F.col("today"), 5)).show(1)

# datediff
dateDF.withColumn("week_ago", F.date_sub(F.col("today"), 7))\
    .select(F.datediff(F.col("week_ago"), F.col("today")))\
    .show(1)

# to_date, months_between
dateDF.select(
    F.to_date(F.lit("2016-01-01")).alias("start"),
    F.to_date(F.lit("2017-05-22")).alias("end"))\
    .select(F.months_between(F.col("start"), F.col("end")))\
    .show(1)

# dateFormat
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    F.to_date(F.lit("2017-12-11"), dateFormat).alias("date"),
    F.to_date(F.lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.show(1)

# to_timestamp
cleanDateDF.select(F.to_timestamp(F.col("date"), dateFormat)).show()
```


## Array Operations

```python
# Column Array - F.array(*cols)
df = df.withColumn('full_name', F.array('fname', 'lname'))

# Empty Array - F.array(*cols)
df = df.withColumn('empty_array_column', F.array([]))
```

## Aggregation Operations

```python
# Count - F.count()
# Sum - F.sum(*cols)
# Mean - F.mean(*cols)
# Max - F.max(*cols)
# Min - F.min(*cols)
df = df.groupBy('gender').agg(F.max('age').alias('max_age_by_gender'))

# Collect Set - F.collect_set(col)
# Collect List - F.collect_list(col)
df = df.groupBy('age').agg(F.collect_set('name').alias('person_names'))
```

## Advanced Operations

#### Repartitioning

```python
# Repartition – df.repartition(num_output_partitions)
df = df.repartition(1)

# Show partition
df.rdd.getNumPartitions()
```

#### UDFs (User Defined Functions)

```python
# Multiply each row's age column by two
times_two_udf = F.udf(lambda x: x * 2)
df = df.withColumn('age', times_two_udf(df.age))

# Randomly choose a value to use as a row's name
import random

random_name_udf = F.udf(lambda: random.choice(['Bob', 'Tom', 'Amy', 'Jenna']))
df = df.withColumn('name', random_name_udf())
```


## SQL

```python
file_path = SPARK_BOOK_DATA_PATH + "/data/flight-data/json/2015-summary.json"
df = spark.read.json(file_path)

# DF => SQL
df.createOrReplaceTempView("some_sql_view") 

# SQL => DF
df = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")\
   .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")

```

## Stream

```python
# schema
file_path = SPARK_BOOK_DATA_PATH + "/data/activity-data/"
static = spark.read.json(file_path)
dataSchema = static.schema

# readStream
streaming = spark.readStream.schema(dataSchema)\
  .option("maxFilesPerTrigger", 1)\
  .json(file_path)

# transform
activityCounts = streaming.groupBy("gt").count()

# writeStream
activityQuery = activityCounts.writeStream.queryName("activity_counts")\
  .format("memory").outputMode("complete")\
  .start()
```

