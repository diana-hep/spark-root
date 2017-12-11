package org.dianahep.sparkroot

// spark related
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.PrunedFilteredScan
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder

// hadoop hdfs 
import org.apache.hadoop.fs.{Path, FileSystem, PathFilter, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

// sparkroot or root4j
import org.dianahep.root4j.core.RootInput
import org.dianahep.root4j._
import org.dianahep.root4j.interfaces._
import org.dianahep.sparkroot.experimental.core._
import org.dianahep.sparkroot.experimental.core.optimizations._

// logging
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

package object experimental {
  @transient lazy val logger = LogManager.getLogger("spark-root")

  val availableOptimizations = (basicPasses :+ PruningPass(null)).map(_.name)

  /**
   * An impolicit DataFrame Reader
   */
  implicit class RootDataFrameReader(reader: DataFrameReader) {
    def root(paths: String*) = reader.format("org.dianahep.sparkroot.experimental").load(paths: _*)
    def root(path: String) = reader.format("org.dianahep.sparkroot.experimental").load(path)
  }
}

/**
 *  Default Source - spark.sqlContext.read.root(filename) will be directed here
 *  DefaultSource is used if no registration of the Source has been explicitly made!!
 */
package experimental {
  sealed trait NoTTreeThrowable {
    seld: Throwable =>
    val optTreeName: Option[String]
  }

  case class NoTTreeException(
      override val optTreeName: Option[String] = None)
      extends Exception(optTreeName match {
        case Some(treeName) => s"No TTree ${treeName} found"
        case None => "No TTree found"
      }) with NoTTreeThrowable;

  /** TTree Iterator */
  class TTreeIterator(
      tree: TTree,
      streamers: Map[String, TStreamerInfo],
      requiredSchema: StructType,
      filters: Array[Filter],
      roptions: ROptions) 
    extends Iterator[Row] {
    private val tt = {
      // build the IR filtering out the unneededtop top columns
      val att = buildATT(tree, streamers, Some(requiredSchema))

      val advancedPasses = Nil :+ PruningPass(requiredSchema)

      // optimize the IR
      val optimizedIR: SRType = att match {
        case root: SRRoot => {
          // NOTE: basicPasses come first!
          val ir = basicPasses.foldLeft(root)({ (tt: core.SRRoot, pass: OptimizationPass) => pass.run(tt, roptions)})
          logger.info(s"Intermediate represenation after basic passes = \n${printATT(ir)}")
          advancedPasses.foldLeft(ir)({(tt: core.SRRoot, pass: OptimizationPass) => 
            pass.run(tt, roptions)})
        }
        case _ => att
      }
      logger.info(s"Optimized Typed Tree = \n${printATT(optimizedIR)}")
      logger.info(s"requiredSchema = \n${requiredSchema.treeString}")
      
      // return the intermediate representation
      optimizedIR
    }
    def hasNext = containsNext(tt)
    def next() = readSparkRow(tt)
  }

  /** Data Source a la parquet */
  class DefaultSource extends FileFormat {
    override def toString: String = "root"

    override def isSplitable(
        spark: SparkSession,
        options: Map[String, String],
        path: Path): Boolean = {
      false
    }

    /** No writing at this point */
    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType): OutputWriterFactory = null

    /** Infer the schema - use the first file in the list */
    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]): Option[StructType] = {
      val roptions = ROptions(options)
      val treeName = roptions.get("tree")

      // some logging
      logger.info(s"options: ${options}")
      logger.info(s"Building the Abstractly Typed Tree... for treeName=$treeName")
      files.map(_.getPath.toString).foreach({x: String => logger.info(s"pathname = $x")})
      
      // open the ROOT file
      val reader = new RootFileReader(files.head.getPath.toString)

      // get the TTree and generate the Typed Tree - intermediate representation
      val optTree = findTree(reader.getTopDir, treeName)
      val att = optTree match {
        case Some(tree) => buildATT(tree, arrangeStreamers(reader), None)
        case None => throw NoTTreeException(treeName)
      }

      // apply optimizations
      val optimizedIR = att match {
        case root: core.SRRoot => 
          basicPasses.foldLeft(root)({case (tt, pass) => pass.run(tt, roptions)})
        case _ => att
      }

      // return the generated schema
      val sparkSchema = buildSparkSchema(optimizedIR)
      Some(sparkSchema)
    }

    /** reading function */
    override def buildReaderWithPartitionValues(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
      logger.info(s"options: ${options}")
      logger.info(s"buildReaderWithPartitionValues...")
      logger.info(s"partitionSchema: ${partitionSchema.fields.map(_.name).toSeq}")
      logger.info(s"requiredSchema: \n${requiredSchema.treeString}")
      logger.info(s"$options")
      
      (file: PartitionedFile) => {
        val treeName = options.get("tree")
        val roptions = ROptions(options)
        var reader: RootFileReader = null;

        //
        // If there is an issue with the file at TStreamerInfo parsing
        // catch the exception and create an empty iterator
        // NOTE: This will work as long as the file is not the head of the inputFiles list
        //
        try {
          reader = new RootFileReader(file.filePath)
        }
        catch {
          case unknown: Throwable => {
            logger.error(s"Exception thrown: " + unknown)
            logger.error(s"Exception in file: ${file.filePath}")
          }
        }

        //
        // basically this is the finally clause...
        // if reader is not null => build the IR/etc.../Row
        // else output an empty Iterator, so that Spark skips it during the xcution
        //
        if (reader != null) {
          val ttree = findTree(reader, treeName) match {
            case Some(tree) => tree
            case None => throw NoTTreeException(treeName)
          }
          val iter = new TTreeIterator(ttree, arrangeStreamers(reader),
            requiredSchema, filters.toArray, roptions)

          new Iterator[InternalRow] {
            // encoder to convert from Row to InternalRow
            private val encoder = RowEncoder(requiredSchema)

            // we have next?
            override def hasNext: Boolean = iter.hasNext

            // get the next element
            override def next(): InternalRow = encoder.toRow(iter.next())
          }
        }
        else 
          new Iterator[InternalRow] {
            override def hasNext: Boolean = false;
            override def next(): InternalRow = null;
          }
      }
    }
  }
}
