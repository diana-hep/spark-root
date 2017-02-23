# Spark ROOT
**Under rapid development :+1:**

**Current Release is on Maven Central: 0.1.7**

Connect [ROOT](https://root.cern.ch/) to [ApacheSpark](http://spark.apache.org/) to be able to read ROOT TTrees, infer the schema and manipulate the data via Spark's DataFrames. Reading is provided by [root4j](https://github.com/diana-hep/root4j).

## Supported Features
- Basic Numerical Types (e.g. Int, Float, Double Byte , Short)
  - Char is represented as Byte therefore also numeric
- Single or Multi TLeaf for a branch
  - C like structs stored for a branch are also supported. These are identified in a TTree as a TBranch with multiple TLeafs.
  - Structs can contiain fixed size arrays of any dimension.
- 1D or N-Dimensional arrays are supported of fixed dimensions and of simple Numerical Types
- Variable Sized Arrays that follows the standard ROOT's convention. (e.g. in the leaflist you can when you include "var1[othervarname]/I" where *othervarname* is the name of the counter).
- A character string terminated by the 0 character is supported

- STL Collections of Basic Types
- Nested STL Collections of Basic Types (vector\<vector\<map\<int, float\> \> \>)
- Composite Classes of Basic Types
- Composite Classes of other Composite
- Composite Classes with STL Collections of Composites as members
- STL Collection of Composites
- STL Collections of Composites with STL Collection of Composite as class member
- TClonesArray of objects derived from TObject, when TClonesArray occupies its own branch (all cases are in development)

## Current Limitations
- Collection of pointers to some classes (base classes!) will not get properly treated! **We stress, we can read such collections/objects - the problem is how to properly nest it into spark's types **
```
class Base {...};
class Derived1 : public Base {...};
class Derived2 : public Base {...};

std::vector<Base*> - at read/run-time can be ...
    1) std::vector<Derived1>
    2) std::vector<Derived2>
    3) std::vector<Base>

Same idea applies to TClonesArray.
```

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

## TODO List
- [ ] **Schema Inferral** - use the TTree with TStreamerInfo to identify the classes and their descriptions to be able to automatically infer the schema from the [ROOT](https://root.cern.ch/) and convert it to Spark's __StructType__
  1. Probably filtering of the columns(branches) that are not needed should be included somehow.
- [ ] **HDFS File Access and Locality** - Extend [root4j](https://github.com/diana-hep/root4j) to read the data on Hadoop Distributed File System. 
- [ ] **Support TRef Functionality** - Allow for the cross-references among columns (example of separate muons and tracks collections, but with internal references from one to the other). We have to be able to programmatically identify these references.
- [ ] **Naming Aliases** - Physics Analysis Specific, full names of objects are typically very long - need aliases to simplify that.
- [ ] **Pruning, Filtering "push-down"** - early stage filtering
- [ ] **Tuning** - tuning file partitioning, etc... The full set of parameters will be identified down the road.
- [ ] **Testing, Testing and once more Testing!**
