# Spark ROOT
**Under rapid development :+1:**

**Current Release is on Maven Central: 0.1.13**

Connect [ROOT](https://root.cern.ch/) to [ApacheSpark](http://spark.apache.org/) to be able to read ROOT TTrees, infer the schema and manipulate the data via Spark's DataFrames/Datasets/RDDs.

## Current Limitations
- Pointers are currently not well supported

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
