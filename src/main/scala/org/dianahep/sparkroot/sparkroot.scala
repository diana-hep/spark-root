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

import org.apache.logging.log4j.scala.Logging

import org.dianahep.root4j.core.RootInput
import org.dianahep.root4j._
import org.dianahep.root4j.interfaces._
import sparkroot.ast._

package object sparkroot {
  /**
   * An impolicit DataFrame Reader
   */
  implicit class RootDataFrameReader(reader: DataFrameReader) {
    def root(paths: String*) = reader.format("org.dianahep.sparkroot").load(paths: _*)
    def root(path: String) = reader.format("org.dianahep.sparkroot").load(path)
  }

  /**
   *  ROOT TTree Iterator
   *  - iterates over the tree
   *  - pump the data from TTree into a spark Row
   */
  class RootTreeIterator(rootTree: TTree, requiredColumns: Array[String], filters: Array[Filter]) extends Iterator[Row] {

    //  Abstract Schema Tree
    private val ast = buildAST(rootTree, null, requiredColumns)

    //  next exists
    def hasNext = containsNext(ast)

    //  get the next Row
    def next() = buildSparkRow(ast)
  }

  /**
   * 1. Builds the Schema
   * 2. Maps execution of each file to a Tree iterator
   */
  class RootTableScan(path: String)(@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan with Logging{
    
    // create the abstract tree
    private val ast: AbstractSchemaTree = 
    {
      //logger.info("Building the Abstract Schema Tree...")
      println("Building the Abstract Schema Tree...")
      val reader = new RootFileReader(new java.io.File(Seq(path) head))
      val tmp = buildAST(findTree(reader.getTopDir), null, null) 
      //logger.info("Done")
      println("Done")
      tmp
    }

    // define the schema from the AST
    def schema: StructType = {
      //logger.info("Building the Spark Schema")
      println("Building the Spark Schema")
      val s = buildSparkSchema(ast)
      //logger.info("Done")
      println("Done")
      s
    }

    // builds a scan
    def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
      println("Building Scan")
      println(requiredColumns.mkString(" "))
      println(filters)

      // parallelize over all the files
      val r = sqlContext.sparkContext.parallelize(Seq(path), 1).
        flatMap({fileName =>
          // TODO: support HDFS (may involve changes to root4j)
          val reader = new RootFileReader(new java.io.File(fileName))
          // TODO: check for multiple trees in the file
          val rootTree = findTree(reader.getTopDir)
          // the real work starts here
          new RootTreeIterator(rootTree, requiredColumns, filters)
        })

      println("Done building Scan")
      r
    }
  }
}

/**
 *  Default Source - spark.sqlContext.read.root(filename) will be directed here!
 */
package sparkroot {
  class DefaultSource extends RelationProvider {
    def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
      new RootTableScan(parameters.getOrElse("path", sys.error("ROOT path must be specified")))(sqlContext)
    }
  }
}
