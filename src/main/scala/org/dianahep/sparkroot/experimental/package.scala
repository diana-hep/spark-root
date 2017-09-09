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
import org.dianahep.sparkroot.core._

// logging
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

package object experimental {
  @transient lazy val logger = LogManager.getLogger("SparkRoot")

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
  /** TTree Iterator */
  class TTreeIterator(
      tree: TTree,
      streamers: Map[String, TStreamerInfo],
      requiredColumns: Array[String],
      filters: Array[Filter]) extends Iterator[Row] {
    private val tt = buildATT(tree, streamers, requiredColumns)
    def hasNext = containsNext(tt)
    def next() = readSparkRow(tt)
  }

  /** Data Source a la parquet */
  class DefaultSource extends FileFormat {
    override def toString: String = "root"

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
      val treeName = options.get("tree")
      logger.info(s"Building the Abstractly Typed Tree... for treeName=$treeName")
      files.map(_.getPath.toString).foreach({x: String => logger.info(s"pathname = $x")})
      val reader = new RootFileReader(files.head.getPath.toString)
      Some(buildSparkSchema(
        buildATT(findTree(reader.getTopDir, treeName), arrangeStreamers(reader), null)))
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
      logger.info(s"buildReaderWithPartitionValues...")
      logger.info(s"${dataSchema.fields.map(_.name).toSeq}")
      logger.info(s"${partitionSchema.fields.map(_.name).toSeq}")
      logger.info(s"${requiredSchema.fields.map(_.name).toSeq}")
      logger.info(s"$options")
//      buildReader(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
      
      
      (file: PartitionedFile) => {
        val treeName = options.get("tree")
        val reader = new RootFileReader(file.filePath)
        val ttree = findTree(reader, treeName);
        val iter = new TTreeIterator(ttree, arrangeStreamers(reader),
          requiredSchema.fields.map(_.name), filters.toArray)

        new Iterator[InternalRow] {
          // encoder to convert from Row to InternalRow
          private val encoder = RowEncoder(requiredSchema)

          // we have next?
          override def hasNext: Boolean = iter.hasNext

          // get the next element
          override def next(): InternalRow = encoder.toRow(iter.next())
        }
      }
    }
  }
}
