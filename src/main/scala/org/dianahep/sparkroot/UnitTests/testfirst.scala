package org.dianahep.sparkroot.UnitTests



import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.dianahep.sparkroot._

import scala.collection.mutable

object testfirst {

  case class myintvectorclass(myintvector : Array[Integer])
  case class myvector2class(myvector2 : Array[Array[Integer]])
  case class vofvofDoubleclass(vofvofDouble : Array[Array[java.lang.Double]])
  //case class Muonsclass(Muons : Array[Array[?]])
  case class aclass(a: Integer)
  case class bclass(b: java.lang.Double)
  case class cclass(c: java.lang.Float)
  case class dclass(d: java.lang.Byte)
  case class fclass(f: java.lang.Boolean)
  case class arr1class(arr1: Array[Integer])
  case class arr2class(arr2: Array[java.lang.Double])
  case class arr3class(arr3: Array[java.lang.Float])
  case class arr4class(arr4: Array[java.lang.Byte])
  case class arr5class(arr5: Array[java.lang.Boolean])
  case class strclass(str: java.lang.String)
  case class multi1class(multi1: Array[Array[Array[Integer]]])
  case class multi2class(multi2: Array[Array[Array[java.lang.Double]]])
  case class multi3class(multi3: Array[Array[Array[java.lang.Float]]])
  case class multi4class(multi4: Array[Array[Array[java.lang.Byte]]])
  case class multi5class(multi5: Array[Array[Array[java.lang.Boolean]]])
  case class nclass(n: Integer)
  case class varr1class(varr1: Array[Integer])
  case class varr2class(varr2: Array[java.lang.Double])

  def main() {

    val inputFileName = "/home/pratyush/CERN/spark-root/src/main/resources/test_root4j.root" //Change to point to project directory
    val conf = new SparkConf().setAppName("Unit Testing test_root4j")
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

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
      var finalarr = new Array[Integer](10000)//Not set to actual upper limit
      var ds = Seq(myintvectorclass(Array()))
      for (i <- 0 to 98){
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
      if (da.except(ds).count() != 0 || ds.except(da).count != 0) {
        println("myintvector Unit Test failed")
        System.exit(0)
      }
    }

    def createdmyvector2() : Dataset[Row] = {
      var ds = Seq[myvector2class]()
      for (i <- 0 to 99){
        var c=0
        var finalarr = new Array[Array[Integer]](40000)//Not set to actual upper limit
        for (j <- 0 to ((4*(i+1))-1)){
          if ((j+1)%2==0){
            var arraytest = new Array[Integer](2)
            arraytest(0)=0
            arraytest(1)=1
            finalarr(c)=arraytest
          }
          else {
            var arraytest=new Array[Integer](1)
            arraytest(0)=0
            finalarr(c)=arraytest
          }
          c=c+1
        }

        var arr = new Array[Array[Integer]](c)
        for (j <- 0 to c-1){
          arr(j)=finalarr(j)
        }
        ds=ds :+ myvector2class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparemyvector2()= {
      val da = df.select("myvector2")
      val ds = createdmyvector2()
      if (da.except(ds).count() != 0 || ds.except(da).count != 0) {
        println("myvector2 Unit Test failed")
        System.exit(0)
      }
    }

    def createdvofvofDouble() : Dataset[Row] = {
      var ds = Seq[vofvofDoubleclass]()
      for (i <- 0 to 99){
        ds = ds :+ vofvofDoubleclass(Array())
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparevofvofDouble()= {
      val da = df.select("vofvofDouble")
      val ds = createdvofvofDouble()
      if (da.except(ds).count() != 0 || ds.except(da).count != 0) {
        println("vofvofDouble Unit Test failed")
        System.exit(0)
      }
    }

    /**def createdMuons() : Dataset[Row] = {
      var ds = Seq[Muonsclass]()
      for (i <- 0 to 99){
        ds = ds :+ Muonsclass(Array())
      }
      import spark.implicits._
      ds.toDF()
    }

    def compareMuons()= {
      val da = df.select("Muons")
      val ds = createdMuons()
      println(da.count())
      println(ds.count())
      da.show(false)
      ds.show(false)
      if (da.except(ds).count() != 0 || ds.except(da).count != 0) {
        println("Muons Unit Test failed")
        System.exit(0)
      }
    }
      **/

    def createda() : Dataset[Row] = {
      var ds = Seq[aclass]()
      for (i <- 0 to 99){
        ds = ds :+ aclass(i)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparea()= {
      val da = df.select("a")
      val ds = createda()
      System.out.println(da.count())
      System.out.println(ds.count())
      da.show()
      ds.show()
      if (da.except(ds).count() != 0 || ds.except(da).count != 0) {
        println("a Unit Test failed")
        System.exit(0)
      }
    }

    comparemyintvector
    comparemyvector2
    comparevofvofDouble
    //compareMuons
    comparea

    flag
  }
}




