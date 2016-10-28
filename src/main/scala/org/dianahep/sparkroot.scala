package org.dianahep

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.PrunedFilteredScan
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.types._

import hep.io.root.core.RootInput
import hep.io.root._
import hep.io.root.interfaces._

package object sparkroot {
  implicit class RootDataFrameReader(reader: DataFrameReader) {
    def root(paths: String*) = reader.format("org.dianahep.sparkroot").load(paths: _*)
    def root(path: String) = reader.format("org.dianahep.sparkroot").load(path)
  }

  def rootTreesInFile(


  def rootTreeIterator(rootTree: TTree, requiredColumns: Array[String], filters: Array[Filter]): Iterator[Row] = {
    println(s"""path: $path\nrequiredColumns: ${requiredColumns.mkString(" ")}\nfilters: ${filters.mkString(" ")}""")









  }



  case class MyTableScan(
    path: String,
    count: Int,
    partitions: Int)
    (@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan
  {
    // hard-coded for now, but generally we'd get this from the TTree
    def schema: StructType =
      StructType(Seq(
        StructField("pileup", IntegerType, nullable = false)// ,
        // StructField("muons", ArrayType(StructType(Seq(
        //   StructField("pt", FloatType, nullable = false),
        //   StructField("eta", FloatType, nullable = false),
        //   StructField("phi", FloatType, nullable = false)), nullable = false),
        //   containsNull = false)),
        // StructField("jets", ArrayType(StructType(Seq(
        //   StructField("pt", FloatType, nullable = false),
        //   StructField("eta", FloatType, nullable = false),
        //   StructField("phi", FloatType, nullable = false)), nullable = false),
        //   containsNull = false))
      ), nullable = false)





    private def makeRow(i: Int): Row = Row(i, i*i, i*i*i)

    def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
      val values = (1 to count).map(i => makeRow(i))
      sqlContext.sparkContext.parallelize(values, partitions)
    }

  }

  class DefaultSource extends RelationProvider {
    def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
      MyTableScan(
        parameters.getOrElse("path", sys.error("ROOT path must be specified")),
        parameters("rows").toInt,
        parameters("partitions").toInt
      )(sqlContext)
    }
  }

}








// package sparkroot {
//   class DefaultSource extends FileFormat with DataSourceRegister {
//     override def shortName(): String = "root"

//     override def inferSchema(
//       sparkSession: SparkSession,
//       options: Map[String, String],
//       files: Seq[FileStatus]): Some[StructType] =
//       // hard-coded for now
//       Some()

//     def prepareWrite(sparkSession: SparkSession,
//       job: Job,
//       options: Map[String, String],
//       dataSchema: StructType): OutputWriterFactory =
//       throw new UnsupportedOperationException(s"buildWriter is not supported for $this")

//     def isSplitable(
//       sparkSession: SparkSession,
//       options: Map[String, String],
//       path: Path): Boolean =
//       false

//     def buildReader(
//       sparkSession: SparkSession,
//       dataSchema: StructType,
//       partitionSchema: StructType,
//       requiredSchema: StructType,
//       filters: Seq[Filter],
//       options: Map[String, String],
//       hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
//       throw new UnsupportedOperationException(s"buildReader is not supported for $this")
//     }




//   }





// }
