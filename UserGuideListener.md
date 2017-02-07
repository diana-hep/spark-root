# Spark ROOT User Guide for basic Performance Tests

**This is in a very early stage of development!**

## Description
This guide is for internal use only (at least at this point).
It aims to point to how to run the most basic performance test (df.count) on CERN's analytix Spark's cluster on a public CMS dataset (1.2TB)

## Requirements
- Apache Spark 2.0 and above
- Scala 2.11
- hdfs 
- access to CERN's lxplus/analytix cluster
- either compile yourself https://github.com/vkhristenko/spark-root-utils or use /afs/cern.ch/work/v/vkhriste/public/spark-root-testdata/jars

## Data
- hdfs:/cms/bigdatasci/vkhriste/data/publiccms\_muionia\_aod

## df.count example
```
on CERN's analytix: you have to source the config (assume that the user knows... for now)

here is the cmd to run the test
./spark-submit --packages org.diana-hep:root4j:0.1.4,org.apache.logging.log4j:log4j:2.8 --jars /afs/cern.ch/work/v/vkhriste/public/spark-root-testdata/jars/spark-root-utils_2.11-0.0.1.jar,/afs/cern.ch/work/v/vkhriste/public/spark-root-testdata/jars/spark-root_2.11-0.1.7.jar --num-executors 10 --executor-cores 4 --conf spark.dynamicAllocation.enabled=false --master yarn  --conf spark.extraListeners=org.dianahep.sparkrootutils.stats.CustomListener --class org.dianahep.sparkrootutils.apps.PerformanceTester /afs/cern.ch/work/v/vkhriste/public/spark-root-testdata/jars/spark-root-utils_2.11-0.0.1.jar 10 20 4 10 output.csv hdfs:/cms/bigdatasci/vkhriste/data/publiccms_muionia_aod

the input arguments are: minExecs maxExecs numCores numTrials outputFile inputFolderFile
```
