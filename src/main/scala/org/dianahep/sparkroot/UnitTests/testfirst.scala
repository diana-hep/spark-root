package org.dianahep.sparkroot.UnitTests

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession, DataFrame}
import org.dianahep.sparkroot._

object testfirst {

  case class generate(myintvector : Array[Int])

  def main() {

    val inputFileName = "/home/pratyush/CERN/spark-root/src/test/resources/test_root4j.root"
    val conf = new SparkConf().setAppName("Unit Testing test_root4j")
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val first = generatedRDD(spark, inputFileName)
    println(first)
    val second = createdRDD(spark)
    println(second)
    if ((first.except(second)).count()==0 && (second.except(first)).count==0){
      System.out.println("Everything is working correctly")
    }
    else {
      System.out.println("Please check errors")
    }
    spark.stop()

  }

  def generatedRDD(spark: SparkSession, inputName: String) : Dataset[Row] = {
    val df = spark.sqlContext.read.root(inputName)
    df.limit(1).select("myintvector")
  }

  def createdRDD(spark: SparkSession) : Dataset[Row] ={
    /**case class generate(myintvector : Array[Int],myvector2 : Array[Array[Int]],vofvofdouble : Array[Array[Double]],
                        Muons: Array[Array[Null]],a: Int, b: Double, c: Float, d: Byte, f: Boolean,
                        arr1 : Array[Int], arr2 : Array[Double],arr3 : Array[Float],arr4: Array[Byte],
                        arr5: Array[Boolean],str: String, multi1 : Array[Array[Array[Int]]],
                        multi2 : Array[Array[Array[Double]]], multi3 : Array[Array[Array[Float]]],
                        multi4 : Array[Array[Array[Byte]]], multi5 : Array[Array[Array[Boolean]]],
                        n: Int, varr1 : Array[Int], varr2 : Array[Double]) //Add someStruct later

      val ds = Seq(generate(Array(null),),
      generate(Array(0),),
      generate(Array(0,0,1),),
      generate(Array(0,0,1,0,1.2),),
      generate(Array(0,0,1,0,1,2,0,1,2,3),),
      generate(Array(0,0,1,0,1,2,0,1,2,3,0,1,2,3,4),),
      generate(Array(0,0,1,0,1,2,0,1,2,3,0,1,2,3,4,0,1,2,3,4,5),),
      generate(Array(0,0,1,0,1,2,0,1,2,3,0,1,2,3,4,0,1,2,3,4,5,0,1,2,3,4,5,6)))
      **/


    import spark.implicits._
    val ds = (Seq(generate(Array()))).toDF()
    ds
  }
}




