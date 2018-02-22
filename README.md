# spark-root
**Under rapid development :+1:**

**Current Release is on Maven Central: 0.1.15 (awaiting 0.1.16)**

[![DOI](https://zenodo.org/badge/72210073.svg)](https://zenodo.org/badge/latestdoi/72210073)

An [Apache Spark's](http://spark.apache.org/) Data Source for [ROOT](https://root.cern.ch/) File Format. Employ `Spark's Datasets` for processing of `ROOT TTrees`.

## Current Limitations
- Pointers are currently not well supported

## Requirements
- Apache Spark 2.0.
- Scala 2.11
- [root4j](https://github.com/diana-hep/root4j) - available on Maven Central

## Get Started with 2 commands locally on your laptop!
- Download `https://spark.apache.org/downloads.html`, `unzip` and `cd` into spark's dir
- Start a scala shell: `./bin/spark-shell --packages org.diana-hep:spark-root_2.11:0.1.15`
- Or Start a python shell: `./bin/pyspark --packages org.diana-hep:spark-root_2.11:0.1.15`

__You are ready to start analyzing__

## ATLAS Open Data 
- Download a `ROOT` file from `http://opendata.cern.ch/record/391`
- Start a spark shell: `./bin/spark-shell --packages org.diana-hep:spark-root_2.11:0.1.15`
```
// read in the file
val df = spark.read.root("file:/Users/vk*.root")
df: org.apache.spark.sql.DataFrame = [runNumber: int, eventNumber: int ... 44 more fields]

scala> df.printSchema
root
 |-- runNumber: integer (nullable = true)
 |-- eventNumber: integer (nullable = true)
 |-- channelNumber: integer (nullable = true)
 |-- mcWeight: float (nullable = true)
 |-- pvxp_n: integer (nullable = true)
 |-- vxp_z: float (nullable = true)
 |-- scaleFactor_PILEUP: float (nullable = true)
 |-- scaleFactor_ELE: float (nullable = true)
 |-- scaleFactor_MUON: float (nullable = true)
 |-- scaleFactor_BTAG: float (nullable = true)
 |-- scaleFactor_TRIGGER: float (nullable = true)
 |-- scaleFactor_JVFSF: float (nullable = true)
 |-- scaleFactor_ZVERTEX: float (nullable = true)
 |-- trigE: boolean (nullable = true)
 |-- trigM: boolean (nullable = true)
 |-- passGRL: boolean (nullable = true)
 |-- hasGoodVertex: boolean (nullable = true)
 |-- lep_n: integer (nullable = true)
 |-- lep_truthMatched: array (nullable = true)
 |    |-- element: boolean (containsNull = true)
 |-- lep_trigMatched: array (nullable = true)
 |    |-- element: short (containsNull = true)
 |-- lep_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_z0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_charge: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_type: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_flag: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_ptcone30: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_etcone20: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_trackd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_tracksigd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- met_et: float (nullable = true)
 |-- met_phi: float (nullable = true)
 |-- jet_n: integer (nullable = true)
 |-- alljet_n: integer (nullable = true)
 |-- jet_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_m: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_jvf: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_trueflav: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_truthMatched: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_SV0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_MV1: array (nullable = true)
 |    |-- element: float (containsNull = true)

 // show the first 10 entries for the column vxp_z
 scala> df.select("vxp_z").show
 17/12/14 12:09:31 WARN util.Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
 +----------+
 |     vxp_z|
 +----------+
 |-12.316585|
 | 22.651913|
 |  67.00033|
 | 25.114586|
 |  5.419942|
 |-29.495798|
 | -41.16512|
 |-34.865147|
 |  -34.4249|
 | -43.56932|
 |-10.688576|
 |-75.542625|
 | 24.876757|
 |-21.913155|
 | -66.78894|
 | -48.26993|
 | 56.751675|
 |-41.679707|
 | 4.6949086|
 | 23.715324|
 +----------+

 // count the number of rows (events) when there are 2 leptons
 scala> df.where("lep_n == 2").count
 res2: Long = 647126
```

## Further examples:
- Image Conversion Pipeline `https://github.com/vkhristenko/MPJRPipeline/blob/master/ipynb/convert2images_python.ipynb`
- Feature Engineering `https://github.com/vkhristenko/MPJRPipeline/blob/master/ipynb/preprocessing_python_noudfs.ipynb`
- Public CMS Open Data Example `https://github.com/diana-hep/spark-root/blob/master/ipynb/publicCMSMuonia_exampleAnalysis_wROOT.ipynb` 
