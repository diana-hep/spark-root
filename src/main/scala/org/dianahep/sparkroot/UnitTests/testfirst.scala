package org.dianahep.sparkroot.UnitTests

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession, DataFrame}
import org.dianahep.sparkroot._

object testfirst {

  case class myintvectorclass(myintvector : Array[Integer])
  case class myvector2class(myvector2 : Array[Array[Integer]])
  case class vofvofdoubleclass(vofvofdouble : Array[Array[Double]])
  case class Muonsclass(Muons : Array[Array[Null]])
  case class aclass(a: Integer)
  case class bclass(b: Double)
  case class cclass(c: Float)
  case class dclass(d: Byte)
  case class fclass(f: Boolean)
  case class arr1class(arr1: Array[Integer])
  case class arr2class(arr2: Array[Double])
  case class arr3class(arr3: Array[Float])
  case class arr4class(arr4: Array[Byte])
  case class arr5class(arr5: Array[Boolean])
  case class strclass(str: String)
  case class multi1class(multi1: Array[Array[Array[Integer]]])
  case class multi2class(multi2: Array[Array[Array[Double]]])
  case class multi3class(multi3: Array[Array[Array[Float]]])
  case class multi4class(multi4: Array[Array[Array[Byte]]])
  case class multi5class(multi5: Array[Array[Array[Boolean]]])
  case class nclass(n: Integer)
  case class varr1class(varr1: Array[Integer])
  case class varr2class(varr2: Array[Double])

  def main() {

    val inputFileName = "/home/pratyush/CERN/spark-root/src/test/resources/test_root4j.root"
    val conf = new SparkConf().setAppName("Unit Testing test_root4j")
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    import spark.implicits._

    if (compare(spark,inputFileName)){
      System.out.println("Everything is working correctly")
    }
    spark.stop()

  }

  def compare(spark: SparkSession, inputName: String) : Boolean = {
    val df = spark.sqlContext.read.root(inputName)
    val flag = true

    def createdmyintvector() : Dataset[Row] = {
      var c= 0;
      var finalarr = new Array[Integer](20000)
      var ds = Seq(myintvectorclass(Array()))
      for (i <- 0 to 100){
        for (j <- 0 to i){
          finalarr(c)=j
          c=c+1
        }
        var arr = new Array[Integer](c)
        for (j <- 0 to c-1){
          arr(j)=finalarr(j)
        }
        ds=ds :+ myintvectorclass(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparemyintvector()= {
      val da = df.select("myintvector")
      val ds = createdmyintvector()
      if (da.except(ds).count() != 0 || ds.except(ds).count != 0) {
        println("myintvector Unit Test failed")
        System.exit(0)
      }
    }


    comparemyintvector
    flag
  }
}




