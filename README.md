# Spark ROOT
**Under rapid development :+1:**

Connect [ROOT](https://root.cern.ch/) to [ApacheSpark](http://spark.apache.org/) to be able to read ROOT TTrees, infer the schema and manipulate the data via Spark's DataFrames. Reading is provided by [root4j](https://github.com/diana-hep/root4j).

## Supported Features
- Basic Numerical Types (e.g. Int, Float, Double Byte , Short)
  - Char is represented as Byte
- Single TLeaf for a branch
  - C like structs stored for a branch (e.g. where leaflist has "var1/I:var2/I") are not yet supported
- 1D or N-Dimensional arrays are supported of fixed dimensions and of simple Numerical Types

## Requirements
- Apache Spark 2.0.
- Scala 2.11
- [root4j](https://github.com/diana-hep/root4j) - available on Maven Central

## Test Example - Schema Inferral
```
./spark-shell --packages org.diana-hep:spark-root_2.11:0.1.0

import org.dianahep.sparkroot._

The file used here is available in the resources of the repo
val df = spark.sqlContext.read.root("path/to/spark-root/src/test/resources/test_basicTypes_NDArrays.root")

The ROOT file contains:
- Simple Numeric Types + Char
- Fixed Dim 1D Arrays of these types
- Fixed Dim ND Arrays of these types

scala> df.printSchema
root
 |-- a: integer (nullable = true)
 |-- b: double (nullable = true)
 |-- c: float (nullable = true)
 |-- d: byte (nullable = true)
 |-- f: boolean (nullable = true)
 |-- arr1: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- arr2: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- arr3: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- arr4: array (nullable = true)
 |    |-- element: byte (containsNull = true)
 |-- arr5: array (nullable = true)
 |    |-- element: boolean (containsNull = true)
 |-- multi1: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: integer (containsNull = true)
 |-- multi2: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: double (containsNull = true)
 |-- multi3: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: float (containsNull = true)
 |-- multi4: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: byte (containsNull = true)
 |-- multi5: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: boolean (containsNull = true)


scala> df.show
+---+----+----+---+-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|  a|   b|   c|  d|    f|                arr1|                arr2|                arr3|                arr4|                arr5|              multi1|              multi2|              multi3|              multi4|              multi5|
+---+----+----+---+-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|  0| 0.0| 0.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  1| 1.0| 1.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  2| 2.0| 2.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  3| 3.0| 3.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  4| 4.0| 4.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  5| 5.0| 5.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  6| 6.0| 6.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  7| 7.0| 7.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  8| 8.0| 8.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
|  9| 9.0| 9.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 10|10.0|10.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 11|11.0|11.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 12|12.0|12.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 13|13.0|13.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 14|14.0|14.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 15|15.0|15.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 16|16.0|16.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 17|17.0|17.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 18|18.0|18.0|120|false|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
| 19|19.0|19.0|120| true|[0, 1, 2, 3, 4, 5...|[0.0, 1.0, 2.0, 3...|[0.0, 1.0, 2.0, 3...|[0, 1, 2, 3, 4, 5...|[false, true, fal...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|[WrappedArray(Wra...|
+---+----+----+---+-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
only showing top 20 rows

```

## Quick Test Example - No Schema inferral 
```
./spark-shell --packages org.diana-hep:spark-root_2.11:0.1-pre1,com.databricks:spark-avro_2.11:3.0.1
import org.dianahep.sparkroot._
val df = spark.sqlContext.read.root("/Users/vk/software/diana-hep/test_data/test_1.root")
------
NOTE: read interface is analogous to AVRO or Parquet, or other formats: json....
------

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

scala> for (x <- df) println(x)
    [WrappedArray([46.21682,0.0,0.23936497,-0.06334016], [40.08369,0.0,1.0085266,-2.7310233])]
    [WrappedArray([29.387526,0.0,-0.8683134,-1.2712506], [12.471459,0.0,-0.98095286,-1.5620013])]
    [WrappedArray([66.35497,0.0,-2.2571073,1.7760249], [14.915713,0.0,-0.019391235,1.1315428])]
    [WrappedArray([39.299873,0.0,-1.3723173,-2.41111], [18.740532,0.0,-1.4823006,-2.3708372])]
    [WrappedArray([44.04615,0.0,0.5730408,3.01369], [42.99739,0.0,1.411812,-0.06999289])]
    [WrappedArray([91.52747,0.0,-2.2693365,-2.974106], [67.13079,0.0,-2.2886407,-2.958496], [46.67421,0.0,-1.9730823,1.4727383])]

------
each WrappedArray corresponds to 1 TTree Entry with 2 or more muon objects per 1 Array.
Can use pattern matching with case classes.
------
scala> case class Muon(pt: Float, q: Int, eta: Float, phi: Float);
scala> case class Event(muons: Seq[Muon]);
scala> val rdd = df.as[Event]
scala> for (x <- rdd) println(x)
    Event(WrappedArray(Muon(46.21682,0,0.23936497,-0.06334016), Muon(40.08369,0,1.0085266,-2.7310233)))
    Event(WrappedArray(Muon(29.387526,0,-0.8683134,-1.2712506), Muon(12.471459,0,-0.98095286,-1.5620013)))
    Event(WrappedArray(Muon(66.35497,0,-2.2571073,1.7760249), Muon(14.915713,0,-0.019391235,1.1315428)))
    Event(WrappedArray(Muon(39.299873,0,-1.3723173,-2.41111), Muon(18.740532,0,-1.4823006,-2.3708372)))

------
this way it is much easier to identify what is what... and makes it much more elegant for analysis
------
```

## TODO List
- [ ] **Schema Inferral** - use the TTree with TStreamerInfo to identify the classes and their descriptions to be able to automatically infer the schema from the [ROOT](https://root.cern.ch/) and convert it to Spark's __StructType__
  1. Probably filtering of the columns(branches) that are not needed should be included somehow.
- [ ] **HDFS File Access and Locality** - Extend [root4j](https://github.com/diana-hep/root4j) to read the data on Hadoop Distributed File System. 
- [ ] **Support TRef Functionality** - Allow for the cross-references among columns (example of separate muons and tracks collections, but with internal references from one to the other). We have to be able to programmatically identify these references.
- [ ] **Naming Aliases** - Physics Analysis Specific, full names of objects are typically very long - need aliases to simplify that.
- [ ] **Pruning, Filtering "push-down"** - early stage filtering
- [ ] **Tuning** - tuning file partitioning, etc... The full set of parameters will be identified down the road.
- [ ] **Testing, Testing and once more Testing!**
