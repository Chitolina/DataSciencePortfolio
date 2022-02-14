---
title: "Big Data - PySpark & SQL"
date: 2021-12-06T19:36:52-03:00
draft: false

# post thumb
image: "images/post/disq.jpg"

# meta description
description: "this is meta description"

# taxonomies
categories: 
  - "Big Data"
  - "SQL"
tags:
  - "SQL"
  - "PySpark"
  - "Python"

# post type
type: "featured"
---

**Description**: In this post we are going to learn about the use of some tools to apply on Big Data, that differs a bit from a normal pandas-csv methodology.
Working with Big Data usually demands more sophisticated software that can deal with the upload and storage of Giga files. This was a great module I had from "Stack Academy - Big Data for Data Scientist" that, for its massive importance and content, I decided to bring to my portfolio.

Without further ado, some tools we are going to use are:  

- **DataBricks**: Azure Databricks is a fully managed service which provides powerful ETL (Extract, Transform and Load), analytics, and machine learning capabilities. It gives a simple collaborative environment to run interactive, and scheduled data analysis workloads.   
 
- **PySpark**: PySpark is a Python-based API for utilizing the Spark framework in combination with Python. While Spark is a Big Data computational engine, Python is a programming language.

- **SQL**: SQL, in full structured query language, computer language designed for eliciting information from databases.

So, let's start with small .json files


```
# Reading the data file
arquivo = "/FileStore/tables/shit/2015_summary.json"
```

**inferSchema** = True. 

A column can be of type String, Double, Long, etc.  Using inferSchema=false (default option) will give a dataframe where all columns are strings   (StringType). Depending on what you want to do, strings may not work.    
For example, if you want to add numbers from different columns, then those columns should be of some numeric type.    

**header** = True. 

This will use the first row in the csv file as the dataframe's column names. Setting header=false (default option) will result in a dataframe with default column names: _c0, _c1, _c2, etc.

```
flightData2015 = spark\
.read.format("csv")\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)
```


```
# print the datatypes from the df columns
flightData2015.printSchema()

root -- DEST_COUNTRY_NAME: string (nullable = true) -- ORIGIN_COUNTRY_NAME: string (nullable = true) -- count: integer (nullable = true)
```


```
# print the type of the feature
type(flightData2015)

pyspark.sql.dataframe.DataFrame
```


```
# return the first 5 lines in array format
flightData2015.take(5)

[Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Romania', count=15), Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Croatia', count=1), Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Ireland', count=344), Row(DEST_COUNTRY_NAME='Egypt', ORIGIN_COUNTRY_NAME='United States', count=15), Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='India', count=62)]
```

```
display(flightData2015.show(3))

DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
    United States|            Romania|   15|
    United States|            Croatia|    1|
    United States|            Ireland|  344|
+-----------------+-------------------+-----+
only showing top 3 rows


```
```
flightData2015.count()

256
```

```
# turning off the inferSchema, we can spot the difference on the loading time

flightData2015 = spark\
.read\
.option("inferSchema", "False")\
.option("header", "True")\
.json(arquivo)
```


```
#  reading the json files from a new dir created on databricks (I'm sorry about the name, I just couldn't change when already made)

df = spark\
.read\
.option("inferSchema", "True")\
.option("header", "True")\
.json("/FileStore/tables/shit/*.json")
```


```
df.show(10)


```


    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|   15|
    |    United States|            Croatia|    1|
    |    United States|            Ireland|  344|
    |            Egypt|      United States|   15|
    |    United States|              India|   62|
    |    United States|          Singapore|    1|
    |    United States|            Grenada|   62|
    |       Costa Rica|      United States|  588|
    |          Senegal|      United States|   40|
    |          Moldova|      United States|    1|
    +-----------------+-------------------+-----+
    only showing top 10 rows
    
 


```
df.count()
 1502
```


```
display(df.head(10))

DEST_COUNTRY_NAME	ORIGIN_COUNTRY_NAME	           count
United States	    Romania	                           1
United States	    Ireland	                         264
United States	    India	                          69
Egypt	            United States	                  24
Equatorial Guinea	United States	                   1
United States	    Singapore	                      25
United States	    Grenada	                          54
Costa Rica	        United States	                 477
Senegal	            United States	                  29
United States	    Marshall Islands	                  44

```


### Now we are going to work with SQL

* When you're working at databricks notebook, we must indicate the reference to the language when they are different from python.


```
%sql 
DROP TABLE IF EXISTS all_files;

# erase the table all_files because I'm going to create a table named all_files on the next block,  
# that is the reason why we need to delete an existing one
```


```
%sql
CREATE TABLE all_files
USING json
OPTIONS (path "/FileStore/tables/shit/*.json", header "true")

# the data from the "shit" folder are going to fill the table "all_files"
```


### Querying the data


```
%sql
SELECT * FROM all_files;

DEST_COUNTRY_NAME	ORIGIN_COUNTRY_NAME	          count
United States	       Romania	                     15
United States	        Croatia	                      1
United States	        Ireland	                    344
Egypt	            United States	                 15
United States	        India	                     62
United States	       Singapore	                      1
United States	        Grenada	                     62
Costa Rica	        United States	                588
Senegal	            United States	                 40
```


```
%sql
SELECT count(*) FROM all_files;
```

```
%sql
-- selecting the avg of the countries

SELECT DEST_COUNTRY_NAME
       ,avg(count) AS Quantidade_Paises
FROM all_files
GROUP BY DEST_COUNTRY_NAME
ORDER BY DEST_COUNTRY_NAME;
```


```
from pyspark.sql.functions import max
df.select(max("count")).take(1)
```


    Out[97]: [Row(max(count)=370002)]



```
# Filtering lines with filter
df.filter("count < 2").show(2)
```


    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Croatia|    1|
    |    United States|          Singapore|    1|
    +-----------------+-------------------+-----+
    only showing top 2 rows
    
    



```
# Where, an alias for the filter method
df.where("count < 2").show(2)
```


    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Croatia|    1|
    |    United States|          Singapore|    1|
    +-----------------+-------------------+-----+
    only showing top 2 rows
    
    


### Manipulating Dataframes


```
df.sort("count").show(5)
```


    +--------------------+-------------------+-----+
    |   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +--------------------+-------------------+-----+
    |Saint Vincent and...|      United States|    1|
    |              Malawi|      United States|    1|
    |            Slovakia|      United States|    1|
    |          Kazakhstan|      United States|    1|
    |       United States|       Saint Martin|    1|
    +--------------------+-------------------+-----+
    only showing top 5 rows
    
    



```
from pyspark.sql.functions import desc, asc, expr

# sorting in desc

df.orderBy(expr("count desc")).show(10)
```


    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |         Suriname|      United States|    1|
    |    United States|             Cyprus|    1|
    |    United States|          Gibraltar|    1|
    |           Cyprus|      United States|    1|
    |          Moldova|      United States|    1|
    |     Burkina Faso|      United States|    1|
    |    United States|            Croatia|    1|
    |         Djibouti|      United States|    1|
    |           Zambia|      United States|    1|
    |    United States|            Estonia|    1|
    +-----------------+-------------------+-----+
    only showing top 10 rows
    
    



```
# descritive statistics
df.describe().show()
```


    +-------+-----------------+-------------------+------------------+
    |summary|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|             count|
    +-------+-----------------+-------------------+------------------+
    |  count|             1502|               1502|              1502|
    |   mean|             null|               null|1718.3189081225032|
    | stddev|             null|               null|22300.368619668894|
    |    min|      Afghanistan|        Afghanistan|                 1|
    |    max|         Zimbabwe|           Zimbabwe|            370002|
    +-------+-----------------+-------------------+------------------+
    
    



```
from pyspark.sql.functions import lower, upper, col
df.select(col("DEST_COUNTRY_NAME"),lower(col("DEST_COUNTRY_NAME")),upper(lower(col("DEST_COUNTRY_NAME")))).show(10)
```


    +-----------------+------------------------+-------------------------------+
    |DEST_COUNTRY_NAME|lower(DEST_COUNTRY_NAME)|upper(lower(DEST_COUNTRY_NAME))|
    +-----------------+------------------------+-------------------------------+
    |    United States|           united states|                  UNITED STATES|
    |    United States|           united states|                  UNITED STATES|
    |    United States|           united states|                  UNITED STATES|
    |            Egypt|                   egypt|                          EGYPT|
    |    United States|           united states|                  UNITED STATES|
    |    United States|           united states|                  UNITED STATES|
    |    United States|           united states|                  UNITED STATES|
    |       Costa Rica|              costa rica|                     COSTA RICA|
    |          Senegal|                 senegal|                        SENEGAL|
    |          Moldova|                 moldova|                        MOLDOVA|
    +-----------------+------------------------+-------------------------------+
    only showing top 10 rows
    
    



```
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
```


    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |       Azerbaijan|      United States|    1|
    |          Belarus|      United States|    1|
    |          Belarus|      United States|    1|
    |           Brunei|      United States|    1|
    |         Bulgaria|      United States|    1|
    +-----------------+-------------------+-----+
    only showing top 5 rows
    
    



```
from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)
```


    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |Equatorial Guinea|      United States|    1|
    +-----------------+-------------------+-----+
    only showing top 2 rows
    
    +-----------------+-------------------+------+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
    +-----------------+-------------------+------+
    |    United States|      United States|370002|
    |    United States|      United States|358354|
    +-----------------+-------------------+------+
    only showing top 2 rows
    
    


### Reading modes
- **permissive**: *Set all fields to NULL when it finds records and situations all records in a column called _corrupt_record* (default).

- **dropMalformed**: *Deletes a corrupted or unreadable line.*

- **failFast**: *Fails immediately when it finds a row it doesn't recognize.*


```
# Reading csv
spark.read.format("csv")
.option("mode", "permissive")
.option("inferSchema", "true")
.option("path", "path/to/file(s)")
.schema(someSchema)
.load()
```


```
df = spark.read.format("csv")\
.option("mode", "permissive")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")
```


```
display(df.head(10))
```


#### Creating a schema
- The **infer_schema** option will not always define the best datatype..
- Improves performance when reading large databases.
- Allows customization of column types.
- It's an important skill that can help you on the app rewriting (pandas code)


```
df.printSchema()
```


    root
     |-- InvoiceNo: string (nullable = true)
     |-- StockCode: string (nullable = true)
     |-- Description: string (nullable = true)
     |-- Quantity: integer (nullable = true)
     |-- InvoiceDate: string (nullable = true)
     |-- UnitPrice: double (nullable = true)
     |-- CustomerID: double (nullable = true)
     |-- Country: string (nullable = true)
    
    



```
# Using the StructType object can make you define the type of each variable. Invoice was a string and now I want pyspark to change to int

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, TimestampType
schema_df = StructType([
    StructField("InvoiceNo", IntegerType()),
    StructField("StockCode", IntegerType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", TimestampType()),
    StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", DoubleType()),
    StructField("Country", StringType())
])


```


```
# checking the schema_df type
type(schema_df)
```


    Out[114]: pyspark.sql.types.StructType



```
# using the schema() parameter

df = spark.read.format("csv")\
.option("header", "True")\
.schema(schema_df)\
.option("timestampFormat",'yyyy-/MM/DD hh:mm:ss')\
.load("/FileStore/tables/bronze/2010_12_01.csv")
```


```
df.printSchema()
```


    root
     |-- InvoiceNo: integer (nullable = true)
     |-- StockCode: integer (nullable = true)
     |-- Description: string (nullable = true)
     |-- Quantity: integer (nullable = true)
     |-- InvoiceDate: timestamp (nullable = true)
     |-- UnitPrice: double (nullable = true)
     |-- CustomerID: double (nullable = true)
     |-- Country: string (nullable = true)
    
    



```
display(df.head(10))

# We defined the StockCode to be integer, but some values have int AND strings mixed, so it will not be able to transform these values into int
# The mixed values are going to be presented AS NULL by the permissive mode.
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>InvoiceNo</th><th>StockCode</th><th>Description</th><th>Quantity</th><th>InvoiceDate</th><th>UnitPrice</th><th>CustomerID</th><th>Country</th></tr></thead><tbody><tr><td>536365</td><td>null</td><td>WHITE HANGING HEART T-LIGHT HOLDER</td><td>6</td><td>2010-12-01T08:26:00.000+0000</td><td>2.55</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536365</td><td>71053</td><td>WHITE METAL LANTERN</td><td>6</td><td>2010-12-01T08:26:00.000+0000</td><td>3.39</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536365</td><td>null</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>8</td><td>2010-12-01T08:26:00.000+0000</td><td>2.75</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536365</td><td>null</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>6</td><td>2010-12-01T08:26:00.000+0000</td><td>3.39</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536365</td><td>null</td><td>RED WOOLLY HOTTIE WHITE HEART.</td><td>6</td><td>2010-12-01T08:26:00.000+0000</td><td>3.39</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536365</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>2</td><td>2010-12-01T08:26:00.000+0000</td><td>7.65</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536365</td><td>21730</td><td>GLASS STAR FROSTED T-LIGHT HOLDER</td><td>6</td><td>2010-12-01T08:26:00.000+0000</td><td>4.25</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536366</td><td>22633</td><td>HAND WARMER UNION JACK</td><td>6</td><td>2010-12-01T08:28:00.000+0000</td><td>1.85</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536366</td><td>22632</td><td>HAND WARMER RED POLKA DOT</td><td>6</td><td>2010-12-01T08:28:00.000+0000</td><td>1.85</td><td>17850.0</td><td>United Kingdom</td></tr><tr><td>536367</td><td>84879</td><td>ASSORTED COLOUR BIRD ORNAMENT</td><td>32</td><td>2010-12-01T08:34:00.000+0000</td><td>1.69</td><td>13047.0</td><td>United Kingdom</td></tr></tbody></table></div>


### What if we switch our reading mode?


```
# Switching to failfast instead of permissive

df = spark.read.format("csv")\
.option("header", "True")\
.schema(schema_df)\
.option("mode","failfast")\
.option("timestampFormat",'yyyy-/MM/DD hh:mm:ss')\
.load("/FileStore/tables/bronze/2010_12_01.csv")
```


```
# StockCode - int
df.printSchema()
```


    root
     |-- InvoiceNo: integer (nullable = true)
     |-- StockCode: integer (nullable = true)
     |-- Description: string (nullable = true)
     |-- Quantity: integer (nullable = true)
     |-- InvoiceDate: timestamp (nullable = true)
     |-- UnitPrice: double (nullable = true)
     |-- CustomerID: double (nullable = true)
     |-- Country: string (nullable = true)
    
    



```
display(df.collect()) # Reading error, failfast does not allow the error that permisssive does
```

### JSON Files


```
df_json = spark.read.format("json")\
.option("mode", "FAILFAST")\
.option("inferSchema", "true")\
.load("/FileStore/tables/shit/2010_summary.json")
```


```
df_json.printSchema()
```


    root
     |-- DEST_COUNTRY_NAME: string (nullable = true)
     |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
     |-- count: long (nullable = true)
    
    



```
display(df_json.head(10))
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>DEST_COUNTRY_NAME</th><th>ORIGIN_COUNTRY_NAME</th><th>count</th></tr></thead><tbody><tr><td>United States</td><td>Romania</td><td>1</td></tr><tr><td>United States</td><td>Ireland</td><td>264</td></tr><tr><td>United States</td><td>India</td><td>69</td></tr><tr><td>Egypt</td><td>United States</td><td>24</td></tr><tr><td>Equatorial Guinea</td><td>United States</td><td>1</td></tr><tr><td>United States</td><td>Singapore</td><td>25</td></tr><tr><td>United States</td><td>Grenada</td><td>54</td></tr><tr><td>Costa Rica</td><td>United States</td><td>477</td></tr><tr><td>Senegal</td><td>United States</td><td>29</td></tr><tr><td>United States</td><td>Marshall Islands</td><td>44</td></tr></tbody></table></div>


### Writing files
- **append** : Adds output files to the list of files that already exist in the location.
- **overwrite** : Overwrite files on destination.
- **erroIfExists** : Issues an error and stops if files already exist in the destination.
- **ignore** : If there is data already in the destination, it does nothing.


```
# writing csv
df.write.format("csv")\
.mode("overwrite") \
.option("sep", ",") \
.save("/FileStore/tables/bronze/saida_2010_12_01.csv")
```


```
file = "/FileStore/tables/bronze/saida_2010_12_01.csv/part-00000-tid-513137111285552141-fa5fcb38-55a1-4a12-ac99-df3fa327627c-83-1-c000.csv"
df = spark.read.format("csv")\
.option("header", "True")\
.option("inferSchema", "True")\
.option("timestampFormat",'yyyy-/MM/DD hh:mm:ss')\
.load(file)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>



```
df.show(10)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>


#### Paralel writing


```
# slicing the csv
# it created a new dir on the bronze dir
df.repartition(5).write.format("csv")\
.mode("overwrite") \
.option("sep", ",") \
.save("/FileStore/tables/bronze/saida_2010_12_01.csv")
```

### Parquet File
  
*  Optimized, column-oriented compression.  
*  Encoded by dictionary.


**Converting .csv to .parquet**

- Dataset .csv from https://www.kaggle.com/nhs/general-practice-prescribing-data


```
# Reading the csv files (>4GB)
df = spark.read.format("csv")\
.option("header", "True")\
.option("inferSchema","True")\
.load("/FileStore/tables/bigdata/*.csv")
```


```
display(df.head(10))
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>practice</th><th>bnf_code</th><th>bnf_name</th><th>items</th><th>nic</th><th>act_cost</th><th>quantity</th></tr></thead><tbody><tr><td>5668</td><td>8092</td><td>592</td><td>2</td><td>44.1</td><td>40.84</td><td>189</td></tr><tr><td>1596</td><td>17512</td><td>16983</td><td>2</td><td>1.64</td><td>1.64</td><td>35</td></tr><tr><td>1596</td><td>25587</td><td>16124</td><td>1</td><td>1.26</td><td>1.28</td><td>42</td></tr><tr><td>1596</td><td>12551</td><td>1282</td><td>2</td><td>0.86</td><td>1.02</td><td>42</td></tr><tr><td>1596</td><td>18938</td><td>10575</td><td>1</td><td>1.85</td><td>1.82</td><td>56</td></tr><tr><td>1596</td><td>8777</td><td>21507</td><td>1</td><td>3.31</td><td>3.18</td><td>56</td></tr><tr><td>1596</td><td>9369</td><td>12008</td><td>1</td><td>63.15</td><td>58.56</td><td>56</td></tr><tr><td>1596</td><td>27926</td><td>17643</td><td>2</td><td>158.66</td><td>147.07</td><td>56</td></tr><tr><td>1596</td><td>26148</td><td>10230</td><td>1</td><td>0.35</td><td>0.44</td><td>14</td></tr><tr><td>1596</td><td>9148</td><td>3381</td><td>1</td><td>0.26</td><td>0.35</td><td>7</td></tr></tbody></table></div>



```
df.printSchema()
```


    root
     |-- practice: string (nullable = true)
     |-- bnf_code: string (nullable = true)
     |-- bnf_name: string (nullable = true)
     |-- items: string (nullable = true)
     |-- nic: string (nullable = true)
     |-- act_cost: string (nullable = true)
     |-- quantity: string (nullable = true)
    
    



```
df.count()
```


    Out[5]: 131020089



```
# writing in parquet file
# try to pay attention on the spark version, always work on the version you converted
df.write.format("parquet")\
.mode("overwrite")\
.save("/FileStore/tables/bronze/df-parquet-file.parquet")
```


```
%fs
ls /FileStore/tables/bronze/df-parquet-file.parquet
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_SUCCESS</td><td>_SUCCESS</td><td>0</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_committed_7787927191925526716</td><td>_committed_7787927191925526716</td><td>3689</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_started_7787927191925526716</td><td>_started_7787927191925526716</td><td>0</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00000-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-65-1-c000.snappy.parquet</td><td>part-00000-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-65-1-c000.snappy.parquet</td><td>43404546</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00001-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-66-1-c000.snappy.parquet</td><td>part-00001-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-66-1-c000.snappy.parquet</td><td>43332747</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00002-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-67-1-c000.snappy.parquet</td><td>part-00002-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-67-1-c000.snappy.parquet</td><td>43509266</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00003-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-68-1-c000.snappy.parquet</td><td>part-00003-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-68-1-c000.snappy.parquet</td><td>43363786</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00004-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-69-1-c000.snappy.parquet</td><td>part-00004-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-69-1-c000.snappy.parquet</td><td>43338818</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00005-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-70-1-c000.snappy.parquet</td><td>part-00005-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-70-1-c000.snappy.parquet</td><td>43321014</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00006-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-71-1-c000.snappy.parquet</td><td>part-00006-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-71-1-c000.snappy.parquet</td><td>43388335</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00007-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-72-1-c000.snappy.parquet</td><td>part-00007-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-72-1-c000.snappy.parquet</td><td>43342070</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00008-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-73-1-c000.snappy.parquet</td><td>part-00008-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-73-1-c000.snappy.parquet</td><td>43611014</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00009-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-74-1-c000.snappy.parquet</td><td>part-00009-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-74-1-c000.snappy.parquet</td><td>43625929</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00010-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-75-1-c000.snappy.parquet</td><td>part-00010-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-75-1-c000.snappy.parquet</td><td>43572548</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00011-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-76-1-c000.snappy.parquet</td><td>part-00011-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-76-1-c000.snappy.parquet</td><td>43734111</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00012-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-77-1-c000.snappy.parquet</td><td>part-00012-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-77-1-c000.snappy.parquet</td><td>43604686</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00013-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-78-1-c000.snappy.parquet</td><td>part-00013-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-78-1-c000.snappy.parquet</td><td>43552717</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00014-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-79-1-c000.snappy.parquet</td><td>part-00014-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-79-1-c000.snappy.parquet</td><td>43564426</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00015-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-80-1-c000.snappy.parquet</td><td>part-00015-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-80-1-c000.snappy.parquet</td><td>43447600</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00016-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-81-1-c000.snappy.parquet</td><td>part-00016-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-81-1-c000.snappy.parquet</td><td>43412781</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00017-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-82-1-c000.snappy.parquet</td><td>part-00017-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-82-1-c000.snappy.parquet</td><td>43260512</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00018-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-83-1-c000.snappy.parquet</td><td>part-00018-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-83-1-c000.snappy.parquet</td><td>43159150</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00019-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-84-1-c000.snappy.parquet</td><td>part-00019-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-84-1-c000.snappy.parquet</td><td>43083278</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00020-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-85-1-c000.snappy.parquet</td><td>part-00020-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-85-1-c000.snappy.parquet</td><td>43560695</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00021-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-86-1-c000.snappy.parquet</td><td>part-00021-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-86-1-c000.snappy.parquet</td><td>43507113</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00022-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-87-1-c000.snappy.parquet</td><td>part-00022-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-87-1-c000.snappy.parquet</td><td>43254574</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00023-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-88-1-c000.snappy.parquet</td><td>part-00023-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-88-1-c000.snappy.parquet</td><td>43169851</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00024-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-89-1-c000.snappy.parquet</td><td>part-00024-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-89-1-c000.snappy.parquet</td><td>43578165</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00025-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-90-1-c000.snappy.parquet</td><td>part-00025-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-90-1-c000.snappy.parquet</td><td>43468721</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00026-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-91-1-c000.snappy.parquet</td><td>part-00026-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-91-1-c000.snappy.parquet</td><td>23472896</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00027-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-92-1-c000.snappy.parquet</td><td>part-00027-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-92-1-c000.snappy.parquet</td><td>22908628</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00028-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-93-1-c000.snappy.parquet</td><td>part-00028-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-93-1-c000.snappy.parquet</td><td>22110866</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00029-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-94-1-c000.snappy.parquet</td><td>part-00029-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-94-1-c000.snappy.parquet</td><td>21915775</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00030-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-95-1-c000.snappy.parquet</td><td>part-00030-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-95-1-c000.snappy.parquet</td><td>21843999</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00031-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-96-1-c000.snappy.parquet</td><td>part-00031-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-96-1-c000.snappy.parquet</td><td>21707042</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00032-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-97-1-c000.snappy.parquet</td><td>part-00032-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-97-1-c000.snappy.parquet</td><td>21592930</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00033-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-98-1-c000.snappy.parquet</td><td>part-00033-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-98-1-c000.snappy.parquet</td><td>21261844</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00034-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-99-1-c000.snappy.parquet</td><td>part-00034-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-99-1-c000.snappy.parquet</td><td>42035540</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00035-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-100-1-c000.snappy.parquet</td><td>part-00035-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-100-1-c000.snappy.parquet</td><td>38396217</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00036-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-101-1-c000.snappy.parquet</td><td>part-00036-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-101-1-c000.snappy.parquet</td><td>18363681</td></tr></tbody></table></div>



```
# reading parquet, way faster than csv and json
# csv took 5min, parquet 96s
df_parquet = spark.read.format("parquet")\
.load("/FileStore/tables/bronze/df-parquet-file.parquet")
```


```
# csv took 2min, parquet 10s
df_parquet.count()
```


    Out[7]: 131020089



```
display(df_parquet.head(10))
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>practice</th><th>bnf_code</th><th>bnf_name</th><th>items</th><th>nic</th><th>act_cost</th><th>quantity</th></tr></thead><tbody><tr><td>3626</td><td>12090</td><td>20521</td><td>3</td><td>8.4</td><td>7.82</td><td>168</td></tr><tr><td>3626</td><td>23511</td><td>11576</td><td>1</td><td>32.18</td><td>29.81</td><td>28</td></tr><tr><td>3626</td><td>14802</td><td>14672</td><td>162</td><td>141.13</td><td>133.93</td><td>4760</td></tr><tr><td>3626</td><td>14590</td><td>10011</td><td>17</td><td>15.01</td><td>14.12</td><td>532</td></tr><tr><td>3626</td><td>24483</td><td>13726</td><td>69</td><td>57.57</td><td>54.67</td><td>2121</td></tr><tr><td>3626</td><td>7768</td><td>22070</td><td>155</td><td>113.03</td><td>109.41</td><td>4144</td></tr><tr><td>3626</td><td>1877</td><td>13598</td><td>102</td><td>68.5</td><td>67.4</td><td>2370</td></tr><tr><td>3626</td><td>18110</td><td>3990</td><td>189</td><td>156.66</td><td>150.44</td><td>5222</td></tr><tr><td>3626</td><td>14058</td><td>2144</td><td>23</td><td>23.52</td><td>22.48</td><td>588</td></tr><tr><td>3626</td><td>4558</td><td>5695</td><td>32</td><td>116.64</td><td>109.21</td><td>756</td></tr></tbody></table></div>



```
# checking files size
display(dbutils.fs.ls("/FileStore/tables/bronze/df-parquet-file.parquet"))
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_SUCCESS</td><td>_SUCCESS</td><td>0</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_committed_7787927191925526716</td><td>_committed_7787927191925526716</td><td>3689</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_started_7787927191925526716</td><td>_started_7787927191925526716</td><td>0</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00000-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-65-1-c000.snappy.parquet</td><td>part-00000-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-65-1-c000.snappy.parquet</td><td>43404546</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00001-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-66-1-c000.snappy.parquet</td><td>part-00001-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-66-1-c000.snappy.parquet</td><td>43332747</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00002-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-67-1-c000.snappy.parquet</td><td>part-00002-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-67-1-c000.snappy.parquet</td><td>43509266</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00003-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-68-1-c000.snappy.parquet</td><td>part-00003-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-68-1-c000.snappy.parquet</td><td>43363786</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00004-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-69-1-c000.snappy.parquet</td><td>part-00004-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-69-1-c000.snappy.parquet</td><td>43338818</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00005-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-70-1-c000.snappy.parquet</td><td>part-00005-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-70-1-c000.snappy.parquet</td><td>43321014</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00006-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-71-1-c000.snappy.parquet</td><td>part-00006-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-71-1-c000.snappy.parquet</td><td>43388335</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00007-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-72-1-c000.snappy.parquet</td><td>part-00007-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-72-1-c000.snappy.parquet</td><td>43342070</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00008-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-73-1-c000.snappy.parquet</td><td>part-00008-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-73-1-c000.snappy.parquet</td><td>43611014</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00009-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-74-1-c000.snappy.parquet</td><td>part-00009-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-74-1-c000.snappy.parquet</td><td>43625929</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00010-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-75-1-c000.snappy.parquet</td><td>part-00010-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-75-1-c000.snappy.parquet</td><td>43572548</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00011-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-76-1-c000.snappy.parquet</td><td>part-00011-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-76-1-c000.snappy.parquet</td><td>43734111</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00012-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-77-1-c000.snappy.parquet</td><td>part-00012-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-77-1-c000.snappy.parquet</td><td>43604686</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00013-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-78-1-c000.snappy.parquet</td><td>part-00013-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-78-1-c000.snappy.parquet</td><td>43552717</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00014-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-79-1-c000.snappy.parquet</td><td>part-00014-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-79-1-c000.snappy.parquet</td><td>43564426</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00015-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-80-1-c000.snappy.parquet</td><td>part-00015-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-80-1-c000.snappy.parquet</td><td>43447600</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00016-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-81-1-c000.snappy.parquet</td><td>part-00016-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-81-1-c000.snappy.parquet</td><td>43412781</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00017-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-82-1-c000.snappy.parquet</td><td>part-00017-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-82-1-c000.snappy.parquet</td><td>43260512</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00018-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-83-1-c000.snappy.parquet</td><td>part-00018-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-83-1-c000.snappy.parquet</td><td>43159150</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00019-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-84-1-c000.snappy.parquet</td><td>part-00019-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-84-1-c000.snappy.parquet</td><td>43083278</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00020-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-85-1-c000.snappy.parquet</td><td>part-00020-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-85-1-c000.snappy.parquet</td><td>43560695</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00021-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-86-1-c000.snappy.parquet</td><td>part-00021-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-86-1-c000.snappy.parquet</td><td>43507113</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00022-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-87-1-c000.snappy.parquet</td><td>part-00022-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-87-1-c000.snappy.parquet</td><td>43254574</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00023-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-88-1-c000.snappy.parquet</td><td>part-00023-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-88-1-c000.snappy.parquet</td><td>43169851</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00024-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-89-1-c000.snappy.parquet</td><td>part-00024-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-89-1-c000.snappy.parquet</td><td>43578165</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00025-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-90-1-c000.snappy.parquet</td><td>part-00025-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-90-1-c000.snappy.parquet</td><td>43468721</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00026-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-91-1-c000.snappy.parquet</td><td>part-00026-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-91-1-c000.snappy.parquet</td><td>23472896</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00027-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-92-1-c000.snappy.parquet</td><td>part-00027-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-92-1-c000.snappy.parquet</td><td>22908628</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00028-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-93-1-c000.snappy.parquet</td><td>part-00028-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-93-1-c000.snappy.parquet</td><td>22110866</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00029-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-94-1-c000.snappy.parquet</td><td>part-00029-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-94-1-c000.snappy.parquet</td><td>21915775</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00030-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-95-1-c000.snappy.parquet</td><td>part-00030-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-95-1-c000.snappy.parquet</td><td>21843999</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00031-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-96-1-c000.snappy.parquet</td><td>part-00031-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-96-1-c000.snappy.parquet</td><td>21707042</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00032-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-97-1-c000.snappy.parquet</td><td>part-00032-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-97-1-c000.snappy.parquet</td><td>21592930</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00033-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-98-1-c000.snappy.parquet</td><td>part-00033-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-98-1-c000.snappy.parquet</td><td>21261844</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00034-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-99-1-c000.snappy.parquet</td><td>part-00034-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-99-1-c000.snappy.parquet</td><td>42035540</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00035-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-100-1-c000.snappy.parquet</td><td>part-00035-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-100-1-c000.snappy.parquet</td><td>38396217</td></tr><tr><td>dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00036-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-101-1-c000.snappy.parquet</td><td>part-00036-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-101-1-c000.snappy.parquet</td><td>18363681</td></tr></tbody></table></div>



```
%scala
// script to get size in Gyga
val path="/FileStore/tables/bronze/df-parquet-file.parquet"
val filelist=dbutils.fs.ls(path)
val df_temp = filelist.toDF()
df_temp.createOrReplaceTempView("adlsSize")
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">path: String = /FileStore/tables/bronze/df-parquet-file.parquet
filelist: Seq[com.databricks.backend.daemon.dbutils.FileInfo] = WrappedArray(FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_SUCCESS, _SUCCESS, 0), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_committed_7787927191925526716, _committed_7787927191925526716, 3689), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/_started_7787927191925526716, _started_7787927191925526716, 0), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00000-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-65-1-c000.snappy.parquet, part-00000-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-65-1-c000.snappy.parquet, 43404546), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00001-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-66-1-c000.snappy.parquet, part-00001-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-66-1-c000.snappy.parquet, 43332747), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00002-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-67-1-c000.snappy.parquet, part-00002-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-67-1-c000.snappy.parquet, 43509266), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00003-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-68-1-c000.snappy.parquet, part-00003-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-68-1-c000.snappy.parquet, 43363786), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00004-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-69-1-c000.snappy.parquet, part-00004-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-69-1-c000.snappy.parquet, 43338818), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00005-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-70-1-c000.snappy.parquet, part-00005-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-70-1-c000.snappy.parquet, 43321014), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00006-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-71-1-c000.snappy.parquet, part-00006-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-71-1-c000.snappy.parquet, 43388335), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00007-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-72-1-c000.snappy.parquet, part-00007-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-72-1-c000.snappy.parquet, 43342070), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00008-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-73-1-c000.snappy.parquet, part-00008-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-73-1-c000.snappy.parquet, 43611014), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00009-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-74-1-c000.snappy.parquet, part-00009-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-74-1-c000.snappy.parquet, 43625929), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00010-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-75-1-c000.snappy.parquet, part-00010-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-75-1-c000.snappy.parquet, 43572548), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00011-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-76-1-c000.snappy.parquet, part-00011-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-76-1-c000.snappy.parquet, 43734111), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00012-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-77-1-c000.snappy.parquet, part-00012-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-77-1-c000.snappy.parquet, 43604686), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00013-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-78-1-c000.snappy.parquet, part-00013-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-78-1-c000.snappy.parquet, 43552717), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00014-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-79-1-c000.snappy.parquet, part-00014-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-79-1-c000.snappy.parquet, 43564426), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00015-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-80-1-c000.snappy.parquet, part-00015-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-80-1-c000.snappy.parquet, 43447600), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00016-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-81-1-c000.snappy.parquet, part-00016-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-81-1-c000.snappy.parquet, 43412781), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00017-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-82-1-c000.snappy.parquet, part-00017-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-82-1-c000.snappy.parquet, 43260512), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00018-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-83-1-c000.snappy.parquet, part-00018-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-83-1-c000.snappy.parquet, 43159150), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00019-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-84-1-c000.snappy.parquet, part-00019-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-84-1-c000.snappy.parquet, 43083278), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00020-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-85-1-c000.snappy.parquet, part-00020-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-85-1-c000.snappy.parquet, 43560695), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00021-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-86-1-c000.snappy.parquet, part-00021-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-86-1-c000.snappy.parquet, 43507113), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00022-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-87-1-c000.snappy.parquet, part-00022-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-87-1-c000.snappy.parquet, 43254574), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00023-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-88-1-c000.snappy.parquet, part-00023-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-88-1-c000.snappy.parquet, 43169851), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00024-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-89-1-c000.snappy.parquet, part-00024-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-89-1-c000.snappy.parquet, 43578165), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00025-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-90-1-c000.snappy.parquet, part-00025-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-90-1-c000.snappy.parquet, 43468721), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00026-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-91-1-c000.snappy.parquet, part-00026-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-91-1-c000.snappy.parquet, 23472896), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00027-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-92-1-c000.snappy.parquet, part-00027-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-92-1-c000.snappy.parquet, 22908628), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00028-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-93-1-c000.snappy.parquet, part-00028-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-93-1-c000.snappy.parquet, 22110866), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00029-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-94-1-c000.snappy.parquet, part-00029-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-94-1-c000.snappy.parquet, 21915775), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00030-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-95-1-c000.snappy.parquet, part-00030-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-95-1-c000.snappy.parquet, 21843999), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00031-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-96-1-c000.snappy.parquet, part-00031-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-96-1-c000.snappy.parquet, 21707042), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00032-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-97-1-c000.snappy.parquet, part-00032-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-97-1-c000.snappy.parquet, 21592930), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00033-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-98-1-c000.snappy.parquet, part-00033-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-98-1-c000.snappy.parquet, 21261844), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00034-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-99-1-c000.snappy.parquet, part-00034-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-99-1-c000.snappy.parquet, 42035540), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00035-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-100-1-c000.snappy.parquet, part-00035-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-100-1-c000.snappy.parquet, 38396217), FileInfo(dbfs:/FileStore/tables/bronze/df-parquet-file.parquet/part-00036-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-101-1-c000.snappy.parquet, part-00036-tid-7787927191925526716-636bf920-d1cf-40ad-8e6b-0c5f2332ad2a-101-1-c000.snappy.parquet, 18363681))
df_temp: org.apache.spark.sql.DataFrame = [path: string, name: string ... 1 more field]
</div>



```
%sql
-- query the created view
-- csv 4GB / parquet 1.3k
select round(sum(size)/(1024*1024*1024),3) as sizeInGB from adlsSize
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>sizeInGB</th></tr></thead><tbody><tr><td>1.308</td></tr></tbody></table></div>


### Spark + PostgreSQL
- Query and writes on a relational database


```
# It is similar to: select * from pg_catalog.pg_tables
# jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/{your_database}?user=stack_user@pgserver-1&password={your_password}&sslmode=require
# We also need to Put our server to receive remote access at the azure server

pgDF = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Newdata2021!&sslmode=require")\
.option("dbtable", "pg_catalog.pg_tables")\
.option("user", "stack_user").option("password", "Newdata2021!").load()

#dbtable= means that I query the 'pg_catalog.pg_tables' and call the function 'select * from pg_catalog.pg_tables'
#infos to access (user and password)
```


```
# printing all the lines
display(pgDF.collect())
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>schemaname</th><th>tablename</th><th>tableowner</th><th>tablespace</th><th>hasindexes</th><th>hasrules</th><th>hastriggers</th><th>rowsecurity</th></tr></thead><tbody><tr><td>public</td><td>produtos</td><td>stack_user</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_statistic</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_foreign_table</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_authid</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_user_mapping</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_subscription</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_largeobject</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_type</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_attribute</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_proc</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_class</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_attrdef</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_constraint</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_inherits</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_index</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_operator</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_opfamily</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_opclass</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_am</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_amop</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_amproc</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_language</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_largeobject_metadata</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_aggregate</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_statistic_ext</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_rewrite</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_trigger</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_event_trigger</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_description</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_cast</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_enum</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_namespace</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_conversion</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_depend</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_database</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_db_role_setting</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_tablespace</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_pltemplate</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_auth_members</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_shdepend</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_shdescription</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_ts_config</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_ts_config_map</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_ts_dict</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_ts_parser</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_ts_template</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_extension</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_foreign_data_wrapper</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_foreign_server</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_policy</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_replication_origin</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_default_acl</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_init_privs</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_seclabel</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_shseclabel</td><td>azure_superuser</td><td>pg_global</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_collation</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_partitioned_table</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_range</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_transform</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_sequence</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_publication</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_publication_rel</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>pg_catalog</td><td>pg_subscription_rel</td><td>azure_superuser</td><td>null</td><td>true</td><td>false</td><td>false</td><td>false</td></tr><tr><td>information_schema</td><td>sql_features</td><td>azure_superuser</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr><tr><td>information_schema</td><td>sql_implementation_info</td><td>azure_superuser</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr><tr><td>information_schema</td><td>sql_languages</td><td>azure_superuser</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr><tr><td>information_schema</td><td>sql_packages</td><td>azure_superuser</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr><tr><td>information_schema</td><td>sql_parts</td><td>azure_superuser</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr><tr><td>information_schema</td><td>sql_sizing</td><td>azure_superuser</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr><tr><td>information_schema</td><td>sql_sizing_profiles</td><td>azure_superuser</td><td>null</td><td>false</td><td>false</td><td>false</td><td>false</td></tr></tbody></table></div>



```
# query from schemaname
pgDF.select("schemaname").distinct().show()
```


    +------------------+
    |        schemaname|
    +------------------+
    |information_schema|
    |            public|
    |        pg_catalog|
    +------------------+
    
    



```
# Specific query
# Useful to avoid "select * from."
pgDF = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Newdata2021!&sslmode=require")\
.option("query", "select schemaname,tablename from pg_catalog.pg_tables")\
.option("user", "stack_user").option("password", "Newdata2021!").load()

# query = specific consulting restricting data and selecting columns, avoiding a broad query
```


```
display(pgDF.collect())
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>schemaname</th><th>tablename</th></tr></thead><tbody><tr><td>public</td><td>produtos</td></tr><tr><td>pg_catalog</td><td>pg_statistic</td></tr><tr><td>pg_catalog</td><td>pg_foreign_table</td></tr><tr><td>pg_catalog</td><td>pg_authid</td></tr><tr><td>pg_catalog</td><td>pg_user_mapping</td></tr><tr><td>pg_catalog</td><td>pg_subscription</td></tr><tr><td>pg_catalog</td><td>pg_largeobject</td></tr><tr><td>pg_catalog</td><td>pg_type</td></tr><tr><td>pg_catalog</td><td>pg_attribute</td></tr><tr><td>pg_catalog</td><td>pg_proc</td></tr><tr><td>pg_catalog</td><td>pg_class</td></tr><tr><td>pg_catalog</td><td>pg_attrdef</td></tr><tr><td>pg_catalog</td><td>pg_constraint</td></tr><tr><td>pg_catalog</td><td>pg_inherits</td></tr><tr><td>pg_catalog</td><td>pg_index</td></tr><tr><td>pg_catalog</td><td>pg_operator</td></tr><tr><td>pg_catalog</td><td>pg_opfamily</td></tr><tr><td>pg_catalog</td><td>pg_opclass</td></tr><tr><td>pg_catalog</td><td>pg_am</td></tr><tr><td>pg_catalog</td><td>pg_amop</td></tr><tr><td>pg_catalog</td><td>pg_amproc</td></tr><tr><td>pg_catalog</td><td>pg_language</td></tr><tr><td>pg_catalog</td><td>pg_largeobject_metadata</td></tr><tr><td>pg_catalog</td><td>pg_aggregate</td></tr><tr><td>pg_catalog</td><td>pg_statistic_ext</td></tr><tr><td>pg_catalog</td><td>pg_rewrite</td></tr><tr><td>pg_catalog</td><td>pg_trigger</td></tr><tr><td>pg_catalog</td><td>pg_event_trigger</td></tr><tr><td>pg_catalog</td><td>pg_description</td></tr><tr><td>pg_catalog</td><td>pg_cast</td></tr><tr><td>pg_catalog</td><td>pg_enum</td></tr><tr><td>pg_catalog</td><td>pg_namespace</td></tr><tr><td>pg_catalog</td><td>pg_conversion</td></tr><tr><td>pg_catalog</td><td>pg_depend</td></tr><tr><td>pg_catalog</td><td>pg_database</td></tr><tr><td>pg_catalog</td><td>pg_db_role_setting</td></tr><tr><td>pg_catalog</td><td>pg_tablespace</td></tr><tr><td>pg_catalog</td><td>pg_pltemplate</td></tr><tr><td>pg_catalog</td><td>pg_auth_members</td></tr><tr><td>pg_catalog</td><td>pg_shdepend</td></tr><tr><td>pg_catalog</td><td>pg_shdescription</td></tr><tr><td>pg_catalog</td><td>pg_ts_config</td></tr><tr><td>pg_catalog</td><td>pg_ts_config_map</td></tr><tr><td>pg_catalog</td><td>pg_ts_dict</td></tr><tr><td>pg_catalog</td><td>pg_ts_parser</td></tr><tr><td>pg_catalog</td><td>pg_ts_template</td></tr><tr><td>pg_catalog</td><td>pg_extension</td></tr><tr><td>pg_catalog</td><td>pg_foreign_data_wrapper</td></tr><tr><td>pg_catalog</td><td>pg_foreign_server</td></tr><tr><td>pg_catalog</td><td>pg_policy</td></tr><tr><td>pg_catalog</td><td>pg_replication_origin</td></tr><tr><td>pg_catalog</td><td>pg_default_acl</td></tr><tr><td>pg_catalog</td><td>pg_init_privs</td></tr><tr><td>pg_catalog</td><td>pg_seclabel</td></tr><tr><td>pg_catalog</td><td>pg_shseclabel</td></tr><tr><td>pg_catalog</td><td>pg_collation</td></tr><tr><td>pg_catalog</td><td>pg_partitioned_table</td></tr><tr><td>pg_catalog</td><td>pg_range</td></tr><tr><td>pg_catalog</td><td>pg_transform</td></tr><tr><td>pg_catalog</td><td>pg_sequence</td></tr><tr><td>pg_catalog</td><td>pg_publication</td></tr><tr><td>pg_catalog</td><td>pg_publication_rel</td></tr><tr><td>pg_catalog</td><td>pg_subscription_rel</td></tr><tr><td>information_schema</td><td>sql_features</td></tr><tr><td>information_schema</td><td>sql_implementation_info</td></tr><tr><td>information_schema</td><td>sql_languages</td></tr><tr><td>information_schema</td><td>sql_packages</td></tr><tr><td>information_schema</td><td>sql_parts</td></tr><tr><td>information_schema</td><td>sql_sizing</td></tr><tr><td>information_schema</td><td>sql_sizing_profiles</td></tr></tbody></table></div>



```
# creating the "produtos" table from the df data
pgDF.write.mode("overwrite")\
.format("jdbc")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Newdata2021!&sslmode=require")\
.option("dbtable", "produtos")\
.option("user", "stack_user")\
.option("password", "Newdata2021!")\
.save()
```


```
# creating the dataframe df_produtos from the created table
df_produtos = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://pgserver-1.postgres.database.azure.com:5432/postgres?user=stack_user@pgserver-1&password=Newdata2021!&sslmode=require")\
.option("dbtable", "produtos")\
.option("user", "stack_user").option("password", "Newdata2021!").load()
```


```
# printing all the rows
display(df_produtos.collect())
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>schemaname</th><th>tablename</th></tr></thead><tbody><tr><td>public</td><td>produtos</td></tr><tr><td>pg_catalog</td><td>pg_statistic</td></tr><tr><td>pg_catalog</td><td>pg_foreign_table</td></tr><tr><td>pg_catalog</td><td>pg_authid</td></tr><tr><td>pg_catalog</td><td>pg_user_mapping</td></tr><tr><td>pg_catalog</td><td>pg_subscription</td></tr><tr><td>pg_catalog</td><td>pg_largeobject</td></tr><tr><td>pg_catalog</td><td>pg_type</td></tr><tr><td>pg_catalog</td><td>pg_attribute</td></tr><tr><td>pg_catalog</td><td>pg_proc</td></tr><tr><td>pg_catalog</td><td>pg_class</td></tr><tr><td>pg_catalog</td><td>pg_attrdef</td></tr><tr><td>pg_catalog</td><td>pg_constraint</td></tr><tr><td>pg_catalog</td><td>pg_inherits</td></tr><tr><td>pg_catalog</td><td>pg_index</td></tr><tr><td>pg_catalog</td><td>pg_operator</td></tr><tr><td>pg_catalog</td><td>pg_opfamily</td></tr><tr><td>pg_catalog</td><td>pg_opclass</td></tr><tr><td>pg_catalog</td><td>pg_am</td></tr><tr><td>pg_catalog</td><td>pg_amop</td></tr><tr><td>pg_catalog</td><td>pg_amproc</td></tr><tr><td>pg_catalog</td><td>pg_language</td></tr><tr><td>pg_catalog</td><td>pg_largeobject_metadata</td></tr><tr><td>pg_catalog</td><td>pg_aggregate</td></tr><tr><td>pg_catalog</td><td>pg_statistic_ext</td></tr><tr><td>pg_catalog</td><td>pg_rewrite</td></tr><tr><td>pg_catalog</td><td>pg_trigger</td></tr><tr><td>pg_catalog</td><td>pg_event_trigger</td></tr><tr><td>pg_catalog</td><td>pg_description</td></tr><tr><td>pg_catalog</td><td>pg_cast</td></tr><tr><td>pg_catalog</td><td>pg_enum</td></tr><tr><td>pg_catalog</td><td>pg_namespace</td></tr><tr><td>pg_catalog</td><td>pg_conversion</td></tr><tr><td>pg_catalog</td><td>pg_depend</td></tr><tr><td>pg_catalog</td><td>pg_database</td></tr><tr><td>pg_catalog</td><td>pg_db_role_setting</td></tr><tr><td>pg_catalog</td><td>pg_tablespace</td></tr><tr><td>pg_catalog</td><td>pg_pltemplate</td></tr><tr><td>pg_catalog</td><td>pg_auth_members</td></tr><tr><td>pg_catalog</td><td>pg_shdepend</td></tr><tr><td>pg_catalog</td><td>pg_shdescription</td></tr><tr><td>pg_catalog</td><td>pg_ts_config</td></tr><tr><td>pg_catalog</td><td>pg_ts_config_map</td></tr><tr><td>pg_catalog</td><td>pg_ts_dict</td></tr><tr><td>pg_catalog</td><td>pg_ts_parser</td></tr><tr><td>pg_catalog</td><td>pg_ts_template</td></tr><tr><td>pg_catalog</td><td>pg_extension</td></tr><tr><td>pg_catalog</td><td>pg_foreign_data_wrapper</td></tr><tr><td>pg_catalog</td><td>pg_foreign_server</td></tr><tr><td>pg_catalog</td><td>pg_policy</td></tr><tr><td>pg_catalog</td><td>pg_replication_origin</td></tr><tr><td>pg_catalog</td><td>pg_default_acl</td></tr><tr><td>pg_catalog</td><td>pg_init_privs</td></tr><tr><td>pg_catalog</td><td>pg_seclabel</td></tr><tr><td>pg_catalog</td><td>pg_shseclabel</td></tr><tr><td>pg_catalog</td><td>pg_collation</td></tr><tr><td>pg_catalog</td><td>pg_partitioned_table</td></tr><tr><td>pg_catalog</td><td>pg_range</td></tr><tr><td>pg_catalog</td><td>pg_transform</td></tr><tr><td>pg_catalog</td><td>pg_sequence</td></tr><tr><td>pg_catalog</td><td>pg_publication</td></tr><tr><td>pg_catalog</td><td>pg_publication_rel</td></tr><tr><td>pg_catalog</td><td>pg_subscription_rel</td></tr><tr><td>information_schema</td><td>sql_features</td></tr><tr><td>information_schema</td><td>sql_implementation_info</td></tr><tr><td>information_schema</td><td>sql_languages</td></tr><tr><td>information_schema</td><td>sql_packages</td></tr><tr><td>information_schema</td><td>sql_parts</td></tr><tr><td>information_schema</td><td>sql_sizing</td></tr><tr><td>information_schema</td><td>sql_sizing_profiles</td></tr></tbody></table></div>


#### Advancing with Pyspark


```
df.show(10)
```


    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    |   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
    |   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    |   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    |   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|
    |   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|
    |   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
    |   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
    |   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    only showing top 10 rows
    
    



```
# I sum the unit price for each country
df.groupBy("Country").sum("UnitPrice").show()
```


    +--------------+------------------+
    |       Country|    sum(UnitPrice)|
    +--------------+------------------+
    |       Germany| 93.82000000000002|
    |        France|             55.29|
    |          EIRE|133.64000000000001|
    |        Norway|            102.67|
    |     Australia|              73.9|
    |United Kingdom|12428.080000000024|
    |   Netherlands|             16.85|
    +--------------+------------------+
    
    



```
# Showing how much data each country has
df.groupBy("Country").count().show()
```


    +--------------+-----+
    |       Country|count|
    +--------------+-----+
    |       Germany|   29|
    |        France|   20|
    |          EIRE|   21|
    |        Norway|   73|
    |     Australia|   14|
    |United Kingdom| 2949|
    |   Netherlands|    2|
    +--------------+-----+
    
    



```
# Now we get the min value by country
df.groupBy("Country").min("UnitPrice").show()
```


    +--------------+--------------+
    |       Country|min(UnitPrice)|
    +--------------+--------------+
    |       Germany|          0.42|
    |        France|          0.42|
    |          EIRE|          0.65|
    |        Norway|          0.29|
    |     Australia|          0.85|
    |United Kingdom|           0.0|
    |   Netherlands|          1.85|
    +--------------+--------------+
    
    



```
# the max price by country
df.groupBy("Country").max("UnitPrice").show()
```


    +--------------+--------------+
    |       Country|max(UnitPrice)|
    +--------------+--------------+
    |       Germany|          18.0|
    |        France|          18.0|
    |          EIRE|          50.0|
    |        Norway|          7.95|
    |     Australia|           8.5|
    |United Kingdom|        607.49|
    |   Netherlands|          15.0|
    +--------------+--------------+
    
    



```
# the average unit price by country
df.groupBy("Country").avg("UnitPrice").show()
```


    +--------------+------------------+
    |       Country|    avg(UnitPrice)|
    +--------------+------------------+
    |       Germany| 3.235172413793104|
    |        France|            2.7645|
    |          EIRE|6.3638095238095245|
    |        Norway|1.4064383561643836|
    |     Australia| 5.278571428571429|
    |United Kingdom|4.2143370634113335|
    |   Netherlands|             8.425|
    +--------------+------------------+
    
    



```
df.groupBy("Country").mean("UnitPrice").show()
```


    +--------------+------------------+
    |       Country|    avg(UnitPrice)|
    +--------------+------------------+
    |       Germany| 3.235172413793104|
    |        France|            2.7645|
    |          EIRE|6.3638095238095245|
    |        Norway|1.4064383561643836|
    |     Australia| 5.278571428571429|
    |United Kingdom|4.2143370634113335|
    |   Netherlands|             8.425|
    +--------------+------------------+
    
    



```
# GroupBy multiple columns
df.groupBy("Country","CustomerID") \
    .sum("UnitPrice") \
    .show()
```


    +--------------+----------+------------------+
    |       Country|CustomerID|    sum(UnitPrice)|
    +--------------+----------+------------------+
    |United Kingdom|   17420.0| 38.99999999999999|
    |United Kingdom|   15922.0|              48.5|
    |United Kingdom|   16250.0|             47.27|
    |United Kingdom|   13065.0| 73.11000000000001|
    |United Kingdom|   18074.0|62.150000000000006|
    |United Kingdom|   16048.0|12.969999999999999|
    |       Germany|   12472.0|             49.45|
    |United Kingdom|   18085.0|              34.6|
    |United Kingdom|   17905.0|109.90000000000003|
    |United Kingdom|   17841.0|254.63999999999982|
    |United Kingdom|   15291.0|               6.0|
    |United Kingdom|   17951.0|22.000000000000004|
    |United Kingdom|   13255.0|27.299999999999997|
    |United Kingdom|   17690.0|              34.8|
    |United Kingdom|   18229.0|             48.65|
    |United Kingdom|   15605.0| 58.20000000000002|
    |United Kingdom|   18011.0| 66.10999999999999|
    |United Kingdom|   17809.0|              1.45|
    |United Kingdom|   14307.0|115.35000000000004|
    |United Kingdom|   13705.0|183.98999999999998|
    +--------------+----------+------------------+
    only showing top 20 rows
    
    


#### Working with data
- There are several functions in Pyspark to manipulate dates and timestamp..
- Avoid writing your own functions for this.
    - current_day():
    - date_format(dateExpr,format):
    - to_date():
    - to_date(column, fmt):
    - add_months(Column, numMonths):
    - date_add(column, days):
    - date_sub(column, days):
    - datediff(end, start)
    - current_timestamp():
    - hour(column):


```
# Bringing the df from bronze and printing it

df = spark.read.format("csv")\
.option("mode", "permissive")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")

df.show()
```


    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    |   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
    |   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    |   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    |   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|
    |   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|
    |   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
    |   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
    |   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|
    |   536367|    22745|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
    |   536367|    22748|POPPY'S PLAYHOUSE...|       6|2010-12-01 08:34:00|      2.1|   13047.0|United Kingdom|
    |   536367|    22749|FELTCRAFT PRINCES...|       8|2010-12-01 08:34:00|     3.75|   13047.0|United Kingdom|
    |   536367|    22310|IVORY KNITTED MUG...|       6|2010-12-01 08:34:00|     1.65|   13047.0|United Kingdom|
    |   536367|    84969|BOX OF 6 ASSORTED...|       6|2010-12-01 08:34:00|     4.25|   13047.0|United Kingdom|
    |   536367|    22623|BOX OF VINTAGE JI...|       3|2010-12-01 08:34:00|     4.95|   13047.0|United Kingdom|
    |   536367|    22622|BOX OF VINTAGE AL...|       2|2010-12-01 08:34:00|     9.95|   13047.0|United Kingdom|
    |   536367|    21754|HOME BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
    |   536367|    21755|LOVE BUILDING BLO...|       3|2010-12-01 08:34:00|     5.95|   13047.0|United Kingdom|
    |   536367|    21777|RECIPE BOX WITH M...|       4|2010-12-01 08:34:00|     7.95|   13047.0|United Kingdom|
    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    only showing top 20 rows
    
    



```
df.printSchema()

# We can notice that the InvoiceDate wasn't recognize as a date, but string
```


    root
     |-- InvoiceNo: string (nullable = true)
     |-- StockCode: string (nullable = true)
     |-- Description: string (nullable = true)
     |-- Quantity: integer (nullable = true)
     |-- InvoiceDate: string (nullable = true)
     |-- UnitPrice: double (nullable = true)
     |-- CustomerID: double (nullable = true)
     |-- Country: string (nullable = true)
    
    



```
from pyspark.sql.functions import *
# importing the date funcions
# current_date()
df.select(current_date().alias("current_date")).show(1) #selecting a column and creating an alias for it
```


    +------------+
    |current_date|
    +------------+
    |  2021-12-06|
    +------------+
    only showing top 1 row
    
    



```
#date_format()
df.select(col("InvoiceDate"), \
          date_format(col("InvoiceDate"), "dd-MM-yyyy hh:mm:ss")\
          .alias("date_format")).show()
#date_format : I call the column I want to format and the formatting type I want my data
```


    +-------------------+-------------------+
    |        InvoiceDate|        date_format|
    +-------------------+-------------------+
    |2010-12-01 08:26:00|01-12-2010 08:26:00|
    |2010-12-01 08:26:00|01-12-2010 08:26:00|
    |2010-12-01 08:26:00|01-12-2010 08:26:00|
    |2010-12-01 08:26:00|01-12-2010 08:26:00|
    |2010-12-01 08:26:00|01-12-2010 08:26:00|
    |2010-12-01 08:26:00|01-12-2010 08:26:00|
    |2010-12-01 08:26:00|01-12-2010 08:26:00|
    |2010-12-01 08:28:00|01-12-2010 08:28:00|
    |2010-12-01 08:28:00|01-12-2010 08:28:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    |2010-12-01 08:34:00|01-12-2010 08:34:00|
    +-------------------+-------------------+
    only showing top 20 rows
    
    



```
# datediff: return the difference between dates
# So I want to compare the current date (days) with my df date
df.select(col("InvoiceDate"),
    datediff(current_date(),col("InvoiceDate")).alias("datediff")  
  ).show()
```


    +-------------------+--------+
    |        InvoiceDate|datediff|
    +-------------------+--------+
    |2010-12-01 08:26:00|    4023|
    |2010-12-01 08:26:00|    4023|
    |2010-12-01 08:26:00|    4023|
    |2010-12-01 08:26:00|    4023|
    |2010-12-01 08:26:00|    4023|
    |2010-12-01 08:26:00|    4023|
    |2010-12-01 08:26:00|    4023|
    |2010-12-01 08:28:00|    4023|
    |2010-12-01 08:28:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    |2010-12-01 08:34:00|    4023|
    +-------------------+--------+
    only showing top 20 rows
    
    



```
#months_between()
df.select(col("InvoiceDate"), 
    months_between(current_date(),col("InvoiceDate")).alias("months_between")  
  ).show()
```


    +-------------------+--------------+
    |        InvoiceDate|months_between|
    +-------------------+--------------+
    |2010-12-01 08:26:00|   132.1499552|
    |2010-12-01 08:26:00|   132.1499552|
    |2010-12-01 08:26:00|   132.1499552|
    |2010-12-01 08:26:00|   132.1499552|
    |2010-12-01 08:26:00|   132.1499552|
    |2010-12-01 08:26:00|   132.1499552|
    |2010-12-01 08:26:00|   132.1499552|
    |2010-12-01 08:28:00|  132.14991039|
    |2010-12-01 08:28:00|  132.14991039|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    |2010-12-01 08:34:00|  132.14977599|
    +-------------------+--------------+
    only showing top 20 rows
    
    



```
# Extracting year, month, next day, week day
df.select(col("InvoiceDate"), 
     year(col("InvoiceDate")).alias("year"), 
     month(col("InvoiceDate")).alias("month"), 
     next_day(col("InvoiceDate"),"Sunday").alias("next_day"), 
     weekofyear(col("InvoiceDate")).alias("weekofyear") 
  ).show(10)
```


    +-------------------+----+-----+----------+----------+
    |        InvoiceDate|year|month|  next_day|weekofyear|
    +-------------------+----+-----+----------+----------+
    |2010-12-01 08:26:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:26:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:26:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:26:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:26:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:26:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:26:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:28:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:28:00|2010|   12|2010-12-05|        48|
    |2010-12-01 08:34:00|2010|   12|2010-12-05|        48|
    +-------------------+----+-----+----------+----------+
    only showing top 10 rows
    
    



```
# Day of the week, day of the month, day of the year
df.select(col("InvoiceDate"),  
     dayofweek(col("InvoiceDate")).alias("dayofweek"), 
     dayofmonth(col("InvoiceDate")).alias("dayofmonth"), 
     dayofyear(col("InvoiceDate")).alias("dayofyear"), 
  ).show()
```


    +-------------------+---------+----------+---------+
    |        InvoiceDate|dayofweek|dayofmonth|dayofyear|
    +-------------------+---------+----------+---------+
    |2010-12-01 08:26:00|        4|         1|      335|
    |2010-12-01 08:26:00|        4|         1|      335|
    |2010-12-01 08:26:00|        4|         1|      335|
    |2010-12-01 08:26:00|        4|         1|      335|
    |2010-12-01 08:26:00|        4|         1|      335|
    |2010-12-01 08:26:00|        4|         1|      335|
    |2010-12-01 08:26:00|        4|         1|      335|
    |2010-12-01 08:28:00|        4|         1|      335|
    |2010-12-01 08:28:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    |2010-12-01 08:34:00|        4|         1|      335|
    +-------------------+---------+----------+---------+
    only showing top 20 rows
    
    



```
# printing current timestamp, it brings the cluster fuse
df.select(current_timestamp().alias("current_timestamp")
  ).show(1,truncate=False)
```


    +-----------------------+
    |current_timestamp      |
    +-----------------------+
    |2021-12-06 13:09:37.916|
    +-----------------------+
    only showing top 1 row
    
    



```
# returning hour, min, second
df.select(col("InvoiceDate"), 
    hour(col("InvoiceDate")).alias("hour"), 
    minute(col("InvoiceDate")).alias("minute"),
    second(col("InvoiceDate")).alias("second") 
  ).show()
```


    +-------------------+----+------+------+
    |        InvoiceDate|hour|minute|second|
    +-------------------+----+------+------+
    |2010-12-01 08:26:00|   8|    26|     0|
    |2010-12-01 08:26:00|   8|    26|     0|
    |2010-12-01 08:26:00|   8|    26|     0|
    |2010-12-01 08:26:00|   8|    26|     0|
    |2010-12-01 08:26:00|   8|    26|     0|
    |2010-12-01 08:26:00|   8|    26|     0|
    |2010-12-01 08:26:00|   8|    26|     0|
    |2010-12-01 08:28:00|   8|    28|     0|
    |2010-12-01 08:28:00|   8|    28|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    |2010-12-01 08:34:00|   8|    34|     0|
    +-------------------+----+------+------+
    only showing top 20 rows
    
    


#### Missing Values with Pyspark


```
# So we are going to bring some examples of data with missing values

display(dbutils.fs.ls("/databricks-datasets"))
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>dbfs:/databricks-datasets/</td><td>databricks-datasets/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/COVID/</td><td>COVID/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/README.md</td><td>README.md</td><td>976</td></tr><tr><td>dbfs:/databricks-datasets/Rdatasets/</td><td>Rdatasets/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/SPARK_README.md</td><td>SPARK_README.md</td><td>3359</td></tr><tr><td>dbfs:/databricks-datasets/adult/</td><td>adult/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/airlines/</td><td>airlines/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/amazon/</td><td>amazon/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/asa/</td><td>asa/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/atlas_higgs/</td><td>atlas_higgs/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/bikeSharing/</td><td>bikeSharing/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/cctvVideos/</td><td>cctvVideos/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/credit-card-fraud/</td><td>credit-card-fraud/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/cs100/</td><td>cs100/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/cs110x/</td><td>cs110x/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/cs190/</td><td>cs190/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/data.gov/</td><td>data.gov/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/definitive-guide/</td><td>definitive-guide/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/delta-sharing/</td><td>delta-sharing/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/flights/</td><td>flights/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/flower_photos/</td><td>flower_photos/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/flowers/</td><td>flowers/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/genomics/</td><td>genomics/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/hail/</td><td>hail/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/iot/</td><td>iot/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/iot-stream/</td><td>iot-stream/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/learning-spark/</td><td>learning-spark/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/learning-spark-v2/</td><td>learning-spark-v2/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/lending-club-loan-stats/</td><td>lending-club-loan-stats/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/med-images/</td><td>med-images/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/mnist-digits/</td><td>mnist-digits/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/news20.binary/</td><td>news20.binary/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/nyctaxi/</td><td>nyctaxi/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/nyctaxi-with-zipcodes/</td><td>nyctaxi-with-zipcodes/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/online_retail/</td><td>online_retail/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/overlap-join/</td><td>overlap-join/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/power-plant/</td><td>power-plant/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/retail-org/</td><td>retail-org/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/rwe/</td><td>rwe/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/sai-summit-2019-sf/</td><td>sai-summit-2019-sf/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/sample_logs/</td><td>sample_logs/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/samples/</td><td>samples/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/sfo_customer_survey/</td><td>sfo_customer_survey/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/sms_spam_collection/</td><td>sms_spam_collection/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/songs/</td><td>songs/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/structured-streaming/</td><td>structured-streaming/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/timeseries/</td><td>timeseries/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/tpch/</td><td>tpch/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/weather/</td><td>weather/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/wiki/</td><td>wiki/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/wikipedia-datasets/</td><td>wikipedia-datasets/</td><td>0</td></tr><tr><td>dbfs:/databricks-datasets/wine-quality/</td><td>wine-quality/</td><td>0</td></tr></tbody></table></div>



```
# inferSchema = True
# header = True

arquivo = "dbfs:/databricks-datasets/flights/"

df = spark \
.read.format("csv")\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)
```


```
df.show()
```


    +--------+-----+--------+------+-----------+
    |    date|delay|distance|origin|destination|
    +--------+-----+--------+------+-----------+
    |01011245|    6|     602|   ABE|        ATL|
    |01020600|   -8|     369|   ABE|        DTW|
    |01021245|   -2|     602|   ABE|        ATL|
    |01020605|   -4|     602|   ABE|        ATL|
    |01031245|   -4|     602|   ABE|        ATL|
    |01030605|    0|     602|   ABE|        ATL|
    |01041243|   10|     602|   ABE|        ATL|
    |01040605|   28|     602|   ABE|        ATL|
    |01051245|   88|     602|   ABE|        ATL|
    |01050605|    9|     602|   ABE|        ATL|
    |01061215|   -6|     602|   ABE|        ATL|
    |01061725|   69|     602|   ABE|        ATL|
    |01061230|    0|     369|   ABE|        DTW|
    |01060625|   -3|     602|   ABE|        ATL|
    |01070600|    0|     369|   ABE|        DTW|
    |01071725|    0|     602|   ABE|        ATL|
    |01071230|    0|     369|   ABE|        DTW|
    |01070625|    0|     602|   ABE|        ATL|
    |01071219|    0|     569|   ABE|        ORD|
    |01080600|    0|     369|   ABE|        DTW|
    +--------+-----+--------+------+-----------+
    only showing top 20 rows
    
    



```
# Filtering the missing values inside my dataset
df.filter("delay is NULL").show()
```


    +--------------------+-----+--------+------+-----------+
    |                date|delay|distance|origin|destination|
    +--------------------+-----+--------+------+-----------+
    |Abbotsford	BC	Can...| null|    null|  null|       null|
    | Aberdeen	SD	USA	ABR| null|    null|  null|       null|
    |  Abilene	TX	USA	ABI| null|    null|  null|       null|
    |    Akron	OH	USA	CAK| null|    null|  null|       null|
    |  Alamosa	CO	USA	ALS| null|    null|  null|       null|
    |   Albany	GA	USA	ABY| null|    null|  null|       null|
    |   Albany	NY	USA	ALB| null|    null|  null|       null|
    |Albuquerque	NM	US...| null|    null|  null|       null|
    |Alexandria	LA	USA...| null|    null|  null|       null|
    |Allentown	PA	USA	ABE| null|    null|  null|       null|
    | Alliance	NE	USA	AIA| null|    null|  null|       null|
    |   Alpena	MI	USA	APN| null|    null|  null|       null|
    |  Altoona	PA	USA	AOO| null|    null|  null|       null|
    | Amarillo	TX	USA	AMA| null|    null|  null|       null|
    |Anahim Lake	BC	Ca...| null|    null|  null|       null|
    |Anchorage	AK	USA	ANC| null|    null|  null|       null|
    | Appleton	WI	USA	ATW| null|    null|  null|       null|
    |Arviat	NWT	Canada...| null|    null|  null|       null|
    |Asheville	NC	USA	AVL| null|    null|  null|       null|
    |    Aspen	CO	USA	ASE| null|    null|  null|       null|
    +--------------------+-----+--------+------+-----------+
    only showing top 20 rows
    
    



```
# similar function 
df.filter(df.delay.isNull()).show(10)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+--------------------+-----+--------+------+-----------+
                date|delay|distance|origin|destination|
+--------------------+-----+--------+------+-----------+
Abbotsford	BC	Can...| null|    null|  null|       null|
 Aberdeen	SD	USA	ABR| null|    null|  null|       null|
  Abilene	TX	USA	ABI| null|    null|  null|       null|
    Akron	OH	USA	CAK| null|    null|  null|       null|
  Alamosa	CO	USA	ALS| null|    null|  null|       null|
   Albany	GA	USA	ABY| null|    null|  null|       null|
   Albany	NY	USA	ALB| null|    null|  null|       null|
Albuquerque	NM	US...| null|    null|  null|       null|
Alexandria	LA	USA...| null|    null|  null|       null|
Allentown	PA	USA	ABE| null|    null|  null|       null|
+--------------------+-----+--------+------+-----------+
only showing top 10 rows

</div>



```
# Filling NULL with 0 value
df.na.fill(value=0).show()
```


    +--------+-----+--------+------+-----------+
    |    date|delay|distance|origin|destination|
    +--------+-----+--------+------+-----------+
    |01011245|    6|     602|   ABE|        ATL|
    |01020600|   -8|     369|   ABE|        DTW|
    |01021245|   -2|     602|   ABE|        ATL|
    |01020605|   -4|     602|   ABE|        ATL|
    |01031245|   -4|     602|   ABE|        ATL|
    |01030605|    0|     602|   ABE|        ATL|
    |01041243|   10|     602|   ABE|        ATL|
    |01040605|   28|     602|   ABE|        ATL|
    |01051245|   88|     602|   ABE|        ATL|
    |01050605|    9|     602|   ABE|        ATL|
    |01061215|   -6|     602|   ABE|        ATL|
    |01061725|   69|     602|   ABE|        ATL|
    |01061230|    0|     369|   ABE|        DTW|
    |01060625|   -3|     602|   ABE|        ATL|
    |01070600|    0|     369|   ABE|        DTW|
    |01071725|    0|     602|   ABE|        ATL|
    |01071230|    0|     369|   ABE|        DTW|
    |01070625|    0|     602|   ABE|        ATL|
    |01071219|    0|     569|   ABE|        ORD|
    |01080600|    0|     369|   ABE|        DTW|
    |01081230|   33|     369|   ABE|        DTW|
    |01080625|    1|     602|   ABE|        ATL|
    |01080607|    5|     569|   ABE|        ORD|
    |01081219|   54|     569|   ABE|        ORD|
    |01091215|   43|     602|   ABE|        ATL|
    |01090600|  151|     369|   ABE|        DTW|
    |01091725|    0|     602|   ABE|        ATL|
    |01091230|   -4|     369|   ABE|        DTW|
    |01090625|    8|     602|   ABE|        ATL|
    |01091219|   83|     569|   ABE|        ORD|
    +--------+-----+--------+------+-----------+
    only showing top 30 rows
    
    



```
# Filling NULL with 0 value (applied just on delay column)
df.na.fill(value=0, subset=['delay']).show()
```


    +--------+-----+--------+------+-----------+
    |    date|delay|distance|origin|destination|
    +--------+-----+--------+------+-----------+
    |01011245|    6|     602|   ABE|        ATL|
    |01020600|   -8|     369|   ABE|        DTW|
    |01021245|   -2|     602|   ABE|        ATL|
    |01020605|   -4|     602|   ABE|        ATL|
    |01031245|   -4|     602|   ABE|        ATL|
    |01030605|    0|     602|   ABE|        ATL|
    |01041243|   10|     602|   ABE|        ATL|
    |01040605|   28|     602|   ABE|        ATL|
    |01051245|   88|     602|   ABE|        ATL|
    |01050605|    9|     602|   ABE|        ATL|
    |01061215|   -6|     602|   ABE|        ATL|
    |01061725|   69|     602|   ABE|        ATL|
    |01061230|    0|     369|   ABE|        DTW|
    |01060625|   -3|     602|   ABE|        ATL|
    |01070600|    0|     369|   ABE|        DTW|
    |01071725|    0|     602|   ABE|        ATL|
    |01071230|    0|     369|   ABE|        DTW|
    |01070625|    0|     602|   ABE|        ATL|
    |01071219|    0|     569|   ABE|        ORD|
    |01080600|    0|     369|   ABE|        DTW|
    +--------+-----+--------+------+-----------+
    only showing top 20 rows
    
    



```
# All the NUll will be filled with empty string
df.na.fill("").show(100)
```


    +--------+-----+--------+------+-----------+
    |    date|delay|distance|origin|destination|
    +--------+-----+--------+------+-----------+
    |01011245|    6|     602|   ABE|        ATL|
    |01020600|   -8|     369|   ABE|        DTW|
    |01021245|   -2|     602|   ABE|        ATL|
    |01020605|   -4|     602|   ABE|        ATL|
    |01031245|   -4|     602|   ABE|        ATL|
    |01030605|    0|     602|   ABE|        ATL|
    |01041243|   10|     602|   ABE|        ATL|
    |01040605|   28|     602|   ABE|        ATL|
    |01051245|   88|     602|   ABE|        ATL|
    |01050605|    9|     602|   ABE|        ATL|
    |01061215|   -6|     602|   ABE|        ATL|
    |01061725|   69|     602|   ABE|        ATL|
    |01061230|    0|     369|   ABE|        DTW|
    |01060625|   -3|     602|   ABE|        ATL|
    |01070600|    0|     369|   ABE|        DTW|
    |01071725|    0|     602|   ABE|        ATL|
    |01071230|    0|     369|   ABE|        DTW|
    |01070625|    0|     602|   ABE|        ATL|
    |01071219|    0|     569|   ABE|        ORD|
    |01080600|    0|     369|   ABE|        DTW|
    |01081230|   33|     369|   ABE|        DTW|
    |01080625|    1|     602|   ABE|        ATL|
    |01080607|    5|     569|   ABE|        ORD|
    |01081219|   54|     569|   ABE|        ORD|
    |01091215|   43|     602|   ABE|        ATL|
    |01090600|  151|     369|   ABE|        DTW|
    |01091725|    0|     602|   ABE|        ATL|
    |01091230|   -4|     369|   ABE|        DTW|
    |01090625|    8|     602|   ABE|        ATL|
    |01091219|   83|     569|   ABE|        ORD|
    |01101215|   -5|     602|   ABE|        ATL|
    |01100600|   -5|     369|   ABE|        DTW|
    |01101725|    7|     602|   ABE|        ATL|
    |01101230|   -8|     369|   ABE|        DTW|
    |01100625|   52|     602|   ABE|        ATL|
    |01101219|    0|     569|   ABE|        ORD|
    |01111215|  127|     602|   ABE|        ATL|
    |01110600|   -9|     369|   ABE|        DTW|
    |01110625|   -4|     602|   ABE|        ATL|
    |01121215|   -5|     602|   ABE|        ATL|
    |01121725|   -1|     602|   ABE|        ATL|
    |01131215|   14|     602|   ABE|        ATL|
    |01130600|   -7|     369|   ABE|        DTW|
    |01131725|   -6|     602|   ABE|        ATL|
    |01131230|  -13|     369|   ABE|        DTW|
    |01130625|   29|     602|   ABE|        ATL|
    |01131219|   -8|     569|   ABE|        ORD|
    |01140600|   -9|     369|   ABE|        DTW|
    |01141725|   -9|     602|   ABE|        ATL|
    |01141230|   -8|     369|   ABE|        DTW|
    |01140625|   -5|     602|   ABE|        ATL|
    |01141219|  -10|     569|   ABE|        ORD|
    |01150600|    0|     369|   ABE|        DTW|
    |01151725|   -6|     602|   ABE|        ATL|
    |01151230|    0|     369|   ABE|        DTW|
    |01150625|    0|     602|   ABE|        ATL|
    |01150607|    0|     569|   ABE|        ORD|
    |01151219|    0|     569|   ABE|        ORD|
    |01161215|  -10|     602|   ABE|        ATL|
    |01160600|   -1|     369|   ABE|        DTW|
    |01161725|   -6|     602|   ABE|        ATL|
    |01161230|   -7|     369|   ABE|        DTW|
    |01160625|   -4|     602|   ABE|        ATL|
    |01161219|   68|     569|   ABE|        ORD|
    |01171215|   -8|     602|   ABE|        ATL|
    |01170600|   -5|     369|   ABE|        DTW|
    |01171725|    5|     602|   ABE|        ATL|
    |01171230|  -10|     369|   ABE|        DTW|
    |01170625|   -6|     602|   ABE|        ATL|
    |01171219|  -10|     569|   ABE|        ORD|
    |01181215|  -12|     602|   ABE|        ATL|
    |01180600|  -13|     369|   ABE|        DTW|
    |01180625|    0|     602|   ABE|        ATL|
    |01191215|  -16|     602|   ABE|        ATL|
    |01191725|   -5|     602|   ABE|        ATL|
    |01201215|   -8|     602|   ABE|        ATL|
    |01201725|   -5|     602|   ABE|        ATL|
    |01201230|  -11|     369|   ABE|        DTW|
    |01200625|   -7|     602|   ABE|        ATL|
    |01201219|   -6|     569|   ABE|        ORD|
    |01210600|   89|     369|   ABE|        DTW|
    |01211725|    0|     602|   ABE|        ATL|
    |01211230|   44|     369|   ABE|        DTW|
    |01210625|   -6|     602|   ABE|        ATL|
    |01211219|    9|     569|   ABE|        ORD|
    |01220600|   80|     369|   ABE|        DTW|
    |01221230|   -5|     369|   ABE|        DTW|
    |01220625|  333|     602|   ABE|        ATL|
    |01220607|  219|     569|   ABE|        ORD|
    |01221219|   15|     569|   ABE|        ORD|
    |01231215|  -12|     602|   ABE|        ATL|
    |01230600|   -3|     369|   ABE|        DTW|
    |01231725|  180|     602|   ABE|        ATL|
    |01231230|   -5|     369|   ABE|        DTW|
    |01230625|   -8|     602|   ABE|        ATL|
    |01231219|  -13|     569|   ABE|        ORD|
    |01241215|  -11|     602|   ABE|        ATL|
    |01240600|   -3|     369|   ABE|        DTW|
    |01241725|    2|     602|   ABE|        ATL|
    |01241230|   -5|     369|   ABE|        DTW|
    +--------+-----+--------+------+-----------+
    only showing top 100 rows
    
    



```
df.filter("delay is NULL").show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+--------------------+-----+--------+------+-----------+
                date|delay|distance|origin|destination|
+--------------------+-----+--------+------+-----------+
Abbotsford	BC	Can...| null|    null|  null|       null|
 Aberdeen	SD	USA	ABR| null|    null|  null|       null|
  Abilene	TX	USA	ABI| null|    null|  null|       null|
    Akron	OH	USA	CAK| null|    null|  null|       null|
  Alamosa	CO	USA	ALS| null|    null|  null|       null|
   Albany	GA	USA	ABY| null|    null|  null|       null|
   Albany	NY	USA	ALB| null|    null|  null|       null|
Albuquerque	NM	US...| null|    null|  null|       null|
Alexandria	LA	USA...| null|    null|  null|       null|
Allentown	PA	USA	ABE| null|    null|  null|       null|
 Alliance	NE	USA	AIA| null|    null|  null|       null|
   Alpena	MI	USA	APN| null|    null|  null|       null|
  Altoona	PA	USA	AOO| null|    null|  null|       null|
 Amarillo	TX	USA	AMA| null|    null|  null|       null|
Anahim Lake	BC	Ca...| null|    null|  null|       null|
Anchorage	AK	USA	ANC| null|    null|  null|       null|
 Appleton	WI	USA	ATW| null|    null|  null|       null|
Arviat	NWT	Canada...| null|    null|  null|       null|
Asheville	NC	USA	AVL| null|    null|  null|       null|
    Aspen	CO	USA	ASE| null|    null|  null|       null|
+--------------------+-----+--------+------+-----------+
only showing top 20 rows

</div>



```
# Removing null rows, may not be the best practice since you remove the fully row
df.na.drop().show()
```


    +--------+-----+--------+------+-----------+
    |    date|delay|distance|origin|destination|
    +--------+-----+--------+------+-----------+
    |01011245|    6|     602|   ABE|        ATL|
    |01020600|   -8|     369|   ABE|        DTW|
    |01021245|   -2|     602|   ABE|        ATL|
    |01020605|   -4|     602|   ABE|        ATL|
    |01031245|   -4|     602|   ABE|        ATL|
    |01030605|    0|     602|   ABE|        ATL|
    |01041243|   10|     602|   ABE|        ATL|
    |01040605|   28|     602|   ABE|        ATL|
    |01051245|   88|     602|   ABE|        ATL|
    |01050605|    9|     602|   ABE|        ATL|
    |01061215|   -6|     602|   ABE|        ATL|
    |01061725|   69|     602|   ABE|        ATL|
    |01061230|    0|     369|   ABE|        DTW|
    |01060625|   -3|     602|   ABE|        ATL|
    |01070600|    0|     369|   ABE|        DTW|
    |01071725|    0|     602|   ABE|        ATL|
    |01071230|    0|     369|   ABE|        DTW|
    |01070625|    0|     602|   ABE|        ATL|
    |01071219|    0|     569|   ABE|        ORD|
    |01080600|    0|     369|   ABE|        DTW|
    +--------+-----+--------+------+-----------+
    only showing top 20 rows
    
    


#### Basic tasks


```
# Adding a column to a df
df = df.withColumn('Nova Coluna', df['delay']+2) # Calling the name of my new column, which are going to present delay +2 
df.show(10)
```


    +--------+-----+--------+------+-----------+-----------+
    |    date|delay|distance|origin|destination|Nova Coluna|
    +--------+-----+--------+------+-----------+-----------+
    |01011245|    6|     602|   ABE|        ATL|        8.0|
    |01020600|   -8|     369|   ABE|        DTW|       -6.0|
    |01021245|   -2|     602|   ABE|        ATL|        0.0|
    |01020605|   -4|     602|   ABE|        ATL|       -2.0|
    |01031245|   -4|     602|   ABE|        ATL|       -2.0|
    |01030605|    0|     602|   ABE|        ATL|        2.0|
    |01041243|   10|     602|   ABE|        ATL|       12.0|
    |01040605|   28|     602|   ABE|        ATL|       30.0|
    |01051245|   88|     602|   ABE|        ATL|       90.0|
    |01050605|    9|     602|   ABE|        ATL|       11.0|
    +--------+-----+--------+------+-----------+-----------+
    only showing top 10 rows
    
    



```
# Renaming a column
df.withColumnRenamed('Nova Coluna','Delay_2').show()
```


    +--------+-----+--------+------+-----------+-------+
    |    date|delay|distance|origin|destination|Delay_2|
    +--------+-----+--------+------+-----------+-------+
    |01011245|    6|     602|   ABE|        ATL|    8.0|
    |01020600|   -8|     369|   ABE|        DTW|   -6.0|
    |01021245|   -2|     602|   ABE|        ATL|    0.0|
    |01020605|   -4|     602|   ABE|        ATL|   -2.0|
    |01031245|   -4|     602|   ABE|        ATL|   -2.0|
    |01030605|    0|     602|   ABE|        ATL|    2.0|
    |01041243|   10|     602|   ABE|        ATL|   12.0|
    |01040605|   28|     602|   ABE|        ATL|   30.0|
    |01051245|   88|     602|   ABE|        ATL|   90.0|
    |01050605|    9|     602|   ABE|        ATL|   11.0|
    |01061215|   -6|     602|   ABE|        ATL|   -4.0|
    |01061725|   69|     602|   ABE|        ATL|   71.0|
    |01061230|    0|     369|   ABE|        DTW|    2.0|
    |01060625|   -3|     602|   ABE|        ATL|   -1.0|
    |01070600|    0|     369|   ABE|        DTW|    2.0|
    |01071725|    0|     602|   ABE|        ATL|    2.0|
    |01071230|    0|     369|   ABE|        DTW|    2.0|
    |01070625|    0|     602|   ABE|        ATL|    2.0|
    |01071219|    0|     569|   ABE|        ORD|    2.0|
    |01080600|    0|     369|   ABE|        DTW|    2.0|
    +--------+-----+--------+------+-----------+-------+
    only showing top 20 rows
    
    



```
# Removing the new column

df = df.drop('Nova Coluna')
df.show(10)
```


    +--------+-----+--------+------+-----------+
    |    date|delay|distance|origin|destination|
    +--------+-----+--------+------+-----------+
    |01011245|    6|     602|   ABE|        ATL|
    |01020600|   -8|     369|   ABE|        DTW|
    |01021245|   -2|     602|   ABE|        ATL|
    |01020605|   -4|     602|   ABE|        ATL|
    |01031245|   -4|     602|   ABE|        ATL|
    |01030605|    0|     602|   ABE|        ATL|
    |01041243|   10|     602|   ABE|        ATL|
    |01040605|   28|     602|   ABE|        ATL|
    |01051245|   88|     602|   ABE|        ATL|
    |01050605|    9|     602|   ABE|        ATL|
    +--------+-----+--------+------+-----------+
    only showing top 10 rows
    
    


#### Working with UDFs
- Integration of code between APIs
- Care must be taken with code performance using UDFs


```
from pyspark.sql.types import LongType

def quadrado(s):
  return s * s
```


```
# registering in spark database and adjusting the returning type

from pyspark.sql.types import LongType
spark.udf.register("Func_Py_Quadrado", quadrado, LongType())

# naming my registered function("Func_PY..."), then calling the function quadrado and the LongType imported
```


    Out[57]: <function __main__.quadrado(s)>



```
# generating random values
spark.range(1, 20).show()
```


    +---+
    | id|
    +---+
    |  1|
    |  2|
    |  3|
    |  4|
    |  5|
    |  6|
    |  7|
    |  8|
    |  9|
    | 10|
    | 11|
    | 12|
    | 13|
    | 14|
    | 15|
    | 16|
    | 17|
    | 18|
    | 19|
    +---+
    
    



```
# create a view "View_temp" that was created from the random values we made 
spark.range(1, 20).createOrReplaceTempView("View_temp")
```


```
%sql
-- Using a python function mixed with SQL code

select id, Func_Py_Quadrado(id) as id_ao_quadrado
from View_temp

-- calling the function (Fun_Py...)and bringing the id value (the square) as id_ao_quadrado
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>id_ao_quadrado</th></tr></thead><tbody><tr><td>1</td><td>1</td></tr><tr><td>2</td><td>4</td></tr><tr><td>3</td><td>9</td></tr><tr><td>4</td><td>16</td></tr><tr><td>5</td><td>25</td></tr><tr><td>6</td><td>36</td></tr><tr><td>7</td><td>49</td></tr><tr><td>8</td><td>64</td></tr><tr><td>9</td><td>81</td></tr><tr><td>10</td><td>100</td></tr><tr><td>11</td><td>121</td></tr><tr><td>12</td><td>144</td></tr><tr><td>13</td><td>169</td></tr><tr><td>14</td><td>196</td></tr><tr><td>15</td><td>225</td></tr><tr><td>16</td><td>256</td></tr><tr><td>17</td><td>289</td></tr><tr><td>18</td><td>324</td></tr><tr><td>19</td><td>361</td></tr></tbody></table></div>


##### FDUs with Dataframes
###### Functions defined by user (FDU)


```
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
Func_Py_Quadrado = udf(quadrado, LongType())

# obviously the square function already exists, but we are going to register as an object type UDF
```


```
# Creating a dataframe from my view

df = spark.table("View_temp")
```


```
df.show()
```


    +---+
    | id|
    +---+
    |  1|
    |  2|
    |  3|
    |  4|
    |  5|
    |  6|
    |  7|
    |  8|
    |  9|
    | 10|
    | 11|
    | 12|
    | 13|
    | 14|
    | 15|
    | 16|
    | 17|
    | 18|
    | 19|
    +---+
    
    



```
display(df.select("id", Func_Py_Quadrado("id").alias("id_quadrado")))

# Calling the Func_... on the "id" feature and naming the result as "id_quadrado"
# So a python function was created and then used inside a df
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>id</th><th>id_quadrado</th></tr></thead><tbody><tr><td>1</td><td>1</td></tr><tr><td>2</td><td>4</td></tr><tr><td>3</td><td>9</td></tr><tr><td>4</td><td>16</td></tr><tr><td>5</td><td>25</td></tr><tr><td>6</td><td>36</td></tr><tr><td>7</td><td>49</td></tr><tr><td>8</td><td>64</td></tr><tr><td>9</td><td>81</td></tr><tr><td>10</td><td>100</td></tr><tr><td>11</td><td>121</td></tr><tr><td>12</td><td>144</td></tr><tr><td>13</td><td>169</td></tr><tr><td>14</td><td>196</td></tr><tr><td>15</td><td>225</td></tr><tr><td>16</td><td>256</td></tr><tr><td>17</td><td>289</td></tr><tr><td>18</td><td>324</td></tr><tr><td>19</td><td>361</td></tr></tbody></table></div>


#### Koalas
- Translation of python code to pyspark
- Koalas is an open source project that offers an immediate replacement for pandas.
- Koalas fills this gap by providing pandas-equivalent APIs that run on Apache Spark.
- Koalas is useful not only for panda users but also for PySpark users.
  - Koalas support many tasks that they need to do with PySpark, for example, plot data directly from a PySpark DataFrame.
- Koalas support SQL directly in their dataframes.


```
import numpy as np
import pandas as pd
import databricks.koalas as ks

# As we can see koalas is a project from databricks
```


```
pdf = pd.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})
```


```
type(pdf)
```


    Out[65]: pandas.core.frame.DataFrame



```
# Creating a koala df
kdf = ks.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})
```


```
type(kdf)
```


    Out[67]: databricks.koalas.frame.DataFrame



```
# Creating a koala df from a pd df
kdf = ks.DataFrame(pdf)
type(kdf)
```


    Out[68]: databricks.koalas.frame.DataFrame



```
# other ways to transform
kdf = ks.from_pandas(pdf)
type(kdf)
```

```
pdf.head()

     A	            B
0	0.813516	0.527949
1	0.683850	0.468637
2	0.215054	0.058884
3	0.576319	0.378034
4	0.313028	0.762469
```


```
kdf.head()

        A	        B
0	0.599634	0.706762
1	0.534563	0.055601
2	0.704126	0.035550
3	0.161522	0.768715
4	0.304619	0.921843
```


```
kdf.describe()

            A	        B
count	5.000000	5.000000
mean	0.460893	0.497694
std	    0.222420	0.420145
min	    0.161522	0.035550
25%	    0.304619	0.055601
50%	    0.534563	0.706762
75%	    0.599634	0.768715
max	    0.704126	0.921843
```

```
# sorting a df
kdf.sort_values(by='B')

        A	        B
2	0.704126	0.035550
1	0.534563	0.055601
0	0.599634	0.706762
3	0.161522	0.768715
4	0.304619	0.921843

```


```
# cell layout setting

from databricks.koalas.config import set_option, get_option
ks.get_option('compute.max_rows')
ks.set_option('compute.max_rows', 2000)
```


```
# slice
kdf[['A', 'B']]

      A             B
0	0.813516	0.527949
1	0.683850	0.468637
2	0.215054	0.058884
3	0.576319	0.378034
4	0.313028	0.762469
```

```
# loc
kdf.loc[1:2]


        A	        B
1	0.683850	0.468637
2	0.215054	0.058884
```

```
# iloc
kdf.iloc[:3, 1:2]

        B
0	0.527949
1	0.468637
2	0.058884
```

** Using python functions with koala**


```
def quadrado(x):
    return x ** 2
```


```
# enabling frame and series data
from databricks.koalas.config import set_option, reset_option
set_option("compute.ops_on_diff_frames", True)
```


```
# creating a column from a python function, and to do that we call apply
kdf['C'] = kdf.A.apply(quadrado)
```


    /databricks/spark/python/pyspark/sql/pandas/functions.py:386: UserWarning: In Python 3.6+ and Spark 3.0+, it is preferred to specify type hints for pandas UDF instead of specifying pandas UDF type which will be deprecated in the future releases. See SPARK-28264 for more details.
      warnings.warn(
    



```
kdf.head()

        A	        B	        C
0	0.813516	0.527949	0.661809
1	0.683850	0.468637	0.467650
2	0.215054	0.058884	0.046248
3	0.576319	0.378034	0.332144
4	0.313028	0.762469	0.097987
```

```
# grouping data
kdf.groupby('A').sum()

               B	           C
   A		
0.813516	0.527949	0.661809
0.683850	0.468637	0.467650
0.215054	0.058884	0.046248
0.576319	0.378034	0.332144
0.313028	0.762469	0.097987
```


```
# grouping multiple columns
kdf.groupby(['A', 'B']).sum()

                            C
    A	        B	
0.813516	0.527949	0.661809
0.683850	0.468637	0.467650
0.215054	0.058884	0.046248
0.576319	0.378034	0.332144
0.313028	0.762469	0.097987

```

**Using SQL on Koalas**

```
# koala df
kdf = ks.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'pig': [20, 18, 489, 675, 1776],
                    'horse': [4, 25, 281, 600, 1900]})
```


```
# query on koala df
ks.sql("SELECT * FROM {kdf} WHERE pig > 100")

    year	pig	   horse
0	2003	489	    281
1	2009	675	    600
2	2014	1776	1900
```

```
# creates a pd df
pdf = pd.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'sheep': [22, 50, 121, 445, 791],
                    'chicken': [250, 326, 589, 1241, 2118]})
```


```
# Query with inner join between pd and koala df
ks.sql('''
    SELECT ks.pig, pd.chicken
    FROM {kdf} ks INNER JOIN {pdf} pd
    ON ks.year = pd.year
    ORDER BY ks.pig, pd.chicken''')

# select the features pig from koala and chicken from pd
# calling koala df to inner join the pd df
# where year = year in both df
# ordering by both tables

    pig	  chicken
0	18	    326
1	20	    250
2	489	    589
3	675	    1241
4	1776	2118
```

```
# converting koalas df to Pyspark df

kdf = ks.DataFrame({'A': [1, 2, 3, 4, 5], 'B': [10, 20, 30, 40, 50]})
pydf = kdf.to_spark()
```

 So now we arrived to the end of our exercise. This was a great module that taught me multiple things about BigData and eased the fear I had about trying to learn this new step of Data Science, and with a bit of study and discipline it may not be a big monster. I hope you enjoyed 👾!
