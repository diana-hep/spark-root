# User Guide on how to set up and run on CERN's Analytix

**CERN's lxplus account and analytix access is assumed!**

## Description
Below we present a short guideline for how to initalize the environment and 
be able to run on Analytix Cluster utilizing Spark ROOT

## Requirements
- access to CERN's lxplus/analytix cluster

## Data
- hdfs:/cms/bigdatasci/vkhriste/data/publiccms\_muionia\_aod - 1.2TB of public Muonia Dataset from 2010

## Important Notes
- If you have some heavy initializations done in your .bashrc, it's possible that some jars will be screened. Clear sign of that is "the exception about unimplemented methods" 

## Step 0
- Clean log in into analytix (spark jobs can be launched from lxplus as well, for simplicity assume logged in into analytix)

## Step 1: Spark on Analytix and other ENVs
- source /cvmfs/sft.cern.ch/lcg/views/dev3/Tue/x86\_64-slc6-gcc49-opt/setup.sh
- after that spark-shell, pyspark, jupyter-notebook, etc... are available in your PATH

## Step 2: HDFS
If you want to have your own copy of settings, proceed with Personal settings.
Otherwise, you can use currently available settings
**This will be available from central /cvmfs location in the near future. Current config on /cvmfs doesn't work 100% yet**

### Personal settings copy
- copy /afs/cern.ch/work/v/vkhriste/public/spark-root-testdata/hadoop-confext.tar.gz to whever you feel convenient (**temporary, current analytix config is throwing errors on reading**)
- tar -zxvf hadoop-confext.tar.gz
- source hadoop-confext/hadoop-setconf.sh analytix

### Current hdfs Settings
- source /afs/cern.ch/work/v/vkhriste/public/spark-root-testdata/hadoop-confext/hadoop-setconf.sh

## Step 3: Kerberize
- kinit

## Step 4: **Required to run pyspark temporarily!**
- Default configurations (residing in /cvmfs) currently mess up python configs
- unset PYSPARK\_PYTHON
- unset PYSPARK\_DRIVER\_PYTHON

## Step 5.1 Spark Scala Shell
- spark-shell --master yarn --num-executors 30 --executor-cores 4 --packages org.diana-hep:spark-root\_2.11:0.1.7 spark.dynamicAllocation.enabled=false
- num-executors - sets up the number of worker nodes
- executor-cores - sets up the number of cores/threads
- packages - enable additional packages(pulled from maven central or those that you compiled locally)
- spark.dynamicAllocation.enabled - this one disables dynamic allocation of executors - you will get right away as many as you requested (otherwise if you do not use them, they will be taken and you will have to request them again)

## or Step 5.2 Spark Python
### Optional: Jupyter Notebook with python
- export PYSPARK\_DRIVER\_PYTHON=/cvmfs/sft-nightlies.cern.ch/lcg/views/dev3/Tue/x86\_64-slc6-gcc49-opt/bin/jupyter-notebook
- export PYSPARK\_DRIVER\_PYTHON\_OPTS="--ip=`hostname` --browser='/dev/null' --port=8888"

### Running pyspark
- command below will launch pyspark and print the url for you to access the jupyter notebooks.
- pyspark --packages org.diana-hep:spark-root\_2.11:0.1.7 --conf spark.dynamicAllocation.enabled=false --executor-cores 4 --num-executors 60 --master yarn
