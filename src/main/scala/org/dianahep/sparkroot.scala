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

import org.dianahep.root4j.core.RootInput
import org.dianahep.root4j._
import org.dianahep.root4j.interfaces._

package object sparkroot {
  implicit class RootDataFrameReader(reader: DataFrameReader) {
    def root(paths: String*) = reader.format("org.dianahep.sparkroot").load(paths: _*)
    def root(path: String) = reader.format("org.dianahep.sparkroot").load(path)
  }

  def rootTreesInFile(reader: RootFileReader) = 
  {
    // TODO: search subdirectories for TTrees
    val dir = reader.get("ntuplemaker_H2DiMuonMaker").asInstanceOf[org.dianahep.root4j.interfaces.TDirectory]
    (0 until dir.nKeys).
      filter(i => dir.getKey(i).getObjectClass.getClassName == "TTree").
      map(i => dir.getKey(i).getObject.asInstanceOf[org.dianahep.root4j.interfaces.TTree])
  }

  class RootBranchIterator(branch: TBranch) extends Iterator[Array[Float]] {
    // TODO: specialize this with [T : TypeTag] and get typeSize from the type
    // this implementation is for T =:= Float

    private val leaf = branch.getLeaves.get(0).asInstanceOf[TLeaf]
    private val startingEntries = branch.getBasketEntry
    private val typeSize = 4  // Float

    private var basket = 0
    private var entry = 0L

    private def lastInBranch = entry == startingEntries(basket + 1) - 1

    // this can be specialized in subclasses
    private def makeOutput(size: Int) = Array.fill[Float](size)(0.0f)

    // as can this
    private def readOne(rootInput: RootInput) = rootInput.readFloat

    def hasNext = basket != startingEntries.size - 1

    def next() = {
      val endPosition =
        if (lastInBranch) {
          // the endPosition comes from a byte marker in the ROOT header
          val rootInput = branch.setPosition(leaf, entry)
          basket += 1   // this is the last entry in the basket, better update the basket number
          rootInput.getLast
        }
        else {
          // the endPosition is where the next entry starts (in this basket)
          val rootInput = branch.setPosition(leaf, entry + 1)
          rootInput.getPosition
        }

      // actually get the data
      val rootInput = branch.setPosition(leaf, entry)
      // create an array with the right size
      val out = makeOutput((endPosition - rootInput.getPosition).toInt / typeSize)
      // fill it (while loops are faster than any Scalarific construct)
      var i = 0
      while (rootInput.getPosition < endPosition) {
        out(i) = readOne(rootInput)
        i += 1
      }
      // update the entry number and return the array
      entry += 1L
      out
    }
  }

  class RootTreeIterator(rootTree: TTree, requiredColumns: Array[String], filters: Array[Filter]) extends Iterator[Row] {
//    private val met = new RootBranchIterator(rootTree.getBranch("Info").getBranchForName("pfMET"))

    private val muonpt = new RootBranchIterator(rootTree.getBranch("Muons").getBranchForName("_pt"))
    private val muonq = new RootBranchIterator(rootTree.getBranch("Muons").getBranchForName("_charge"))
    private val muoneta = new RootBranchIterator(rootTree.getBranch("Muons").getBranchForName("_eta"))
    private val muonphi = new RootBranchIterator(rootTree.getBranch("Muons").getBranchForName("_phi"))
/*    private val muoneta = new RootBranchIterator(rootTree.getBranch("Muon").getBranchForName("eta"))
    private val muonphi = new RootBranchIterator(rootTree.getBranch("Muon").getBranchForName("phi"))

    private val jetpt = new RootBranchIterator(rootTree.getBranch("AK4CHS").getBranchForName("pt"))
    private val jeteta = new RootBranchIterator(rootTree.getBranch("AK4CHS").getBranchForName("eta"))
    private val jetphi = new RootBranchIterator(rootTree.getBranch("AK4CHS").getBranchForName("phi"))
    */

    def hasNext = muonpt.hasNext

    def next() = {
      val muonpts = muonpt.next()
      val muonqs = muonq.next()
      val muonetas = muoneta.next()
      val muonphis = muonphi.next()
      val muon = Array.fill[Row](muonpts.size)(null)
      var muoni = 0
      while (muoni < muonpts.size) {
        muon(muoni) = Row(muonpts(muoni), muonqs(muoni), muonetas(muoni), muonphis(muoni))
//        muon(muoni) = Row(muonpts(muoni))
        muoni += 1
      }
/*
      val jetpts = jetpt.next()
      val jetetas = jeteta.next()
      val jetphis = jetphi.next()
      val jet = Array.fill[Row](jetpts.size)(null)
      var jeti = 0
      while (jeti < jetpts.size) {
        jet(jeti) = Row(jetpts(jeti), jetetas(jeti), jetphis(jeti))
        jeti += 1
      }*/
      
      //Row(met.next().head, muon, jet)
      Row(muon)
    }
  }

  class RootTableScan(path: String)(@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan {
    // hard-coded for now, but generally we'd get this from the TTree
    def schema: StructType =
      StructType(Seq(
//        StructField("met", FloatType, nullable = false),
        StructField("muons", ArrayType(StructType(Seq(
          StructField("pt", FloatType, nullable = false),
          StructField("q", FloatType, nullable=false),
          StructField("eta", FloatType, nullable = false),
          StructField("phi", FloatType, nullable = false))),
          containsNull = false), nullable = false)
  //      StructField("jets", ArrayType(StructType(Seq(
  //        StructField("pt", FloatType, nullable = false),
  //        StructField("eta", FloatType, nullable = false),
  //        StructField("phi", FloatType, nullable = false))),
  //        containsNull = false), nullable = false)
      ))

    def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
      // TODO: do a glob file pattern on the path and parallelize over all the names
      sqlContext.sparkContext.parallelize(Seq(path), 1).
        flatMap({fileName =>
          // TODO: support HDFS (may involve changes to root4j)
          val reader = new RootFileReader(new java.io.File(fileName))
          // TODO: check for multiple trees in the file
          val rootTree = rootTreesInFile(reader).head
          // the real work starts here
          new RootTreeIterator(rootTree, requiredColumns, filters)
        })
  }
}

package sparkroot {
  class DefaultSource extends RelationProvider {
    def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
      new RootTableScan(parameters.getOrElse("path", sys.error("ROOT path must be specified")))(sqlContext)
    }
  }
}
