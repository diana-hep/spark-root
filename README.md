# Spark ROOT
**Under rapid development :+1:**

Connect [ROOT](https://root.cern.ch/) to [ApacheSpark](http://spark.apache.org/) to be able to read ROOT TTrees, infer the schema and manipulate the data via Spark's DataFrames. Reading is provided by [root4j](https://github.com/diana-hep/root4j).

## Requirements
- Apache Spark 2.0.
- Scala 2.11

## Quick Test Example
```
./spark-shell --packages org.diana-hep:spark-root_2.11:0.1-pre1,com.databricks:spark-avro_2.11:3.0.1
import org.dianahep.sparkroot._
val df = spark.sqlContext.read.root("/Users/vk/software/diana-hep/test_data/test_1.root")

scala> df show
warning: there was one feature warning; re-run with -feature for details
+--------------------+
|               muons|
+--------------------+
|[[46.21682,0.0,0....|
|[[29.387526,0.0,-...|
|[[66.35497,0.0,-2...|
|[[39.299873,0.0,-...|
|[[44.04615,0.0,0....|
|[[91.52747,0.0,-2...|
|[[37.770947,0.0,1...|
|[[55.39022,0.0,-2...|
|[[63.379906,0.0,-...|
|[[32.845856,0.0,-...|
|[[32.77975,0.0,-1...|
|[[118.41255,0.0,0...|
|[[47.456944,0.0,1...|
|[[39.395023,0.0,-...|
|[[231.34692,0.0,-...|
|[[108.01287,0.0,0...|
|[[34.297974,0.0,2...|
|[[50.622807,0.0,2...|
|[[43.058167,0.0,-...|
|[[47.028915,0.0,1...|
+--------------------+
only showing top 20 rows
```

## TODO list
