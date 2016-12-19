package org.dianahep.sparkroot.apps

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

//import org.apache.spark.implicits._

import org.dianahep.sparkroot._

/**
 * A simple example of reading a ROOT file into Spark's DataFrame.
 * Define a case class to properly cast the Dataset[X]
 * and print it out
 *
 * @author Viktor Khristenko
 */

object HiggsExampleApp {
  case class Object();
  case class Track(obj: Object, charge: Int, pt: Float, pterr: Float, eta: Float, phi: Float);
  case class Electron(track: Track, ids: Seq[Boolean], trackIso: Float, ecalIso: Float, hcalIso: Float, dz: Double, isPF: Boolean, convVeto: Boolean);
  case class Event(electrons: Seq[Electron]);

  def main(args: Array[String]) {
    if (args.size!=0) {
      val inputFileName = args(0)
      val conf = new SparkConf().setAppName("Higgs Example Application")
      val spark = SparkSession.builder()
        .master("local")
        .appName("Higgs Example Application")
        .getOrCreate()

      doWork(spark, inputFileName)
      spark.stop()
    }
    else {
      println("No ROOT file provided")
    }
  }

  def doWork(spark: SparkSession, inputName: String) = {
    // load the ROOT file
    val df = spark.sqlContext.read.root(inputName)
    
    // see https://issues.apache.org/jira/browse/SPARK-13540
    import spark.implicits._

    // build the RDD out of the Dataset and filter out right away
    val rdd = df.select("Electrons").as[Event].filter(_.electrons.size!=0).rdd

    // print all the events where electrons are present
    for (x <- rdd) println(x)
  }
}
