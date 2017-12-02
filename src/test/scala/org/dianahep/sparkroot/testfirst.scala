package org.dianahep.sparkroot

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.scalatest._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }
}

case class myintvectorclass(myintvector: Array[Integer])

case class myvector2class(myvector2: Array[Array[Integer]])

case class vofvofDoubleclass(vofvofDouble: Array[Array[java.lang.Double]])

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

abstract class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors

class FirstSpec extends UnitSpec with SparkSessionTestWrapper{
  "Generated dataframe" should "be the same as created dataframe" in {
    val test = new testfirst()
    test.main()
  }
}

class testfirst extends UnitSpec with SparkSessionTestWrapper with DataFrameComparer{


  def main() {
    val url = getClass.getResource("/test_root4j.root")
    val inputFileName = url.getPath()
    val conf = new SparkConf().setAppName("Unit Testing test_root4j")
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    val df = spark.sqlContext.read.root(inputFileName)

    def createdmyintvector(): Dataset[Row] = {
      var c = 0;
      var finalarr = new Array[Integer](10000)
      //Not set to actual upper limit
      var ds = Seq(myintvectorclass(Array()))
      for (i <- 0 to 98) {
        for (j <- 0 to i) {
          finalarr(c) = j
          c = c + 1
        }
        var arr = new Array[Integer](c)
        for (j <- 0 to c - 1) {
          arr(j) = finalarr(j)
        }
        ds = ds :+ myintvectorclass(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparemyintvector() = {
      val da = df.select("myintvector")
      val ds = createdmyintvector()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdmyvector2(): Dataset[Row] = {
      var ds = Seq[myvector2class]()
      for (i <- 0 to 99) {
        var c = 0
        var finalarr = new Array[Array[Integer]](40000) //Not set to actual upper limit
        for (j <- 0 to ((4 * (i + 1)) - 1)) {
          if ((j + 1) % 2 == 0) {
            var arraytest = new Array[Integer](2)
            arraytest(0) = 0
            arraytest(1) = 1
            finalarr(c) = arraytest
          }
          else {
            var arraytest = new Array[Integer](1)
            arraytest(0) = 0
            finalarr(c) = arraytest
          }
          c = c + 1
        }

        var arr = new Array[Array[Integer]](c)
        for (j <- 0 to c - 1) {
          arr(j) = finalarr(j)
        }
        ds = ds :+ myvector2class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparemyvector2() = {
      val da = df.select("myvector2")
      val ds = createdmyvector2()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdvofvofDouble(): Dataset[Row] = {
      var ds = Seq[vofvofDoubleclass]()
      for (i <- 0 to 99) {
        ds = ds :+ vofvofDoubleclass(Array())
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparevofvofDouble() = {
      val da = df.select("vofvofDouble")
      val ds = createdvofvofDouble()
      assertLargeDataFrameEquality(da,ds)
    }

    /** def createdMuons() : Dataset[Row] = {
      * var ds = Seq[Muonsclass]()
      * for (i <- 0 to 99){
      * ds = ds :+ Muonsclass(Array())
      * }
      * import spark.implicits._
      *ds.toDF()
      * }
      **
      *def compareMuons()= {
      * val da = df.select("Muons")
      * val ds = createdMuons()
      * println(da.count())
      * println(ds.count())
      *da.show(false)
      *ds.show(false)
      * if (da.except(ds).count() != 0 || ds.except(da).count != 0) {
      * println("Muons Unit Test failed")
      *System.exit(0)
      * }
      * }
      * */

    def createda(): Dataset[Row] = {
      var ds = Seq[aclass]()
      for (i <- 0 to 99) {
        ds = ds :+ aclass(i)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparea() = {
      val da = df.select("a")
      val ds = createda()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdb(): Dataset[Row] = {
      var ds = Seq[bclass]()
      var c = 0.0
      for (i <- 0 to 99) {
        ds = ds :+ bclass(c)
        c = c + 1.0
      }
      import spark.implicits._
      ds.toDF()
    }

    def compareb() = {
      val da = df.select("b")
      val ds = createdb()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdc(): Dataset[Row] = {
      var ds = Seq[cclass]()
      var c: Float = 0
      for (i <- 0 to 99) {
        ds = ds :+ cclass(c)
        c = c + 1
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparec() = {
      val da = df.select("c")
      val ds = createdc()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdd(): Dataset[Row] = {
      var ds = Seq[dclass]()
      var c: Byte = 120
      for (i <- 0 to 99) {
        ds = ds :+ dclass(c)
      }
      import spark.implicits._
      ds.toDF()
    }

    def compared() = {
      val da = df.select("d")
      val ds = createdd()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdf(): Dataset[Row] = {
      var ds = Seq[fclass]()
      var c = true
      var d = false
      for (i <- 0 to 99) {
        if ((i + 1) % 2 != 0) {
          ds = ds :+ fclass(d)
        }
        else {
          ds = ds :+ fclass(c)
        }
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparef() = {
      val da = df.select("f")
      val ds = createdf()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdarr1(): Dataset[Row] = {
      var ds = Seq[arr1class]()
      for (i <- 0 to 99) {
        var arr = new Array[Integer](100)
        for (j <- 0 to 99) {
          if (j == 1) {
            arr(j) = 1
          }
          else if (j == 68) {
            arr(j) = -554688857
          }
          else if (j == 69) {
            arr(j) = 1375775489
          }
          else if (j == 70) {
            arr(j) = 1389532384
          }
          else if (j == 71) {
            arr(j) = 32767
          }
          else if (j == 72) {
            arr(j) = 37
          }
          else if (j == 73) {
            arr(j) = 0
          }
          else if (j == 74) {
            arr(j) = 1
          }
          else if (j == 75) {
            arr(j) = 0
          }
          else if (j == 76) {
            arr(j) = 216883304
          }
          else if (j == 77) {
            arr(j) = 1
          }
          else if (j == 78) {
            arr(j) = 1642884191
          }
          else if (j == 79) {
            arr(j) = 32767
          }
          else if (j == 80) {
            arr(j) = 1389531888
          }
          else if (j == 81) {
            arr(j) = 32767
          }
          else if (j == 82) {
            arr(j) = 1642726006
          }
          else if (j == 83) {
            arr(j) = 32767
          }
          else if (j == 84) {
            arr(j) = 1389531920
          }
          else if (j == 85) {
            arr(j) = 32767
          }
          else if (j == 86) {
            arr(j) = 1389531920
          }
          else if (j == 87) {
            arr(j) = 32767
          }
          else if (j == 88) {
            arr(j) = 1
          }
          else if (j == 89) {
            arr(j) = 0
          }
          else if (j == 90) {
            arr(j) = 1389531944
          }
          else if (j == 91) {
            arr(j) = 32767
          }
          else if (j == 92) {
            arr(j) = 216883200
          }
          else if (j == 93) {
            arr(j) = 1
          }
          else if (j == 94) {
            arr(j) = 1642721864
          }
          else if (j == 95) {
            arr(j) = 32767
          }
          else if (j == 96) {
            arr(j) = 1389531960
          }
          else if (j == 97) {
            arr(j) = 32767
          }
          else {
            arr(j) = 0
          }
        }
        ds = ds :+ arr1class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparearr1() = {
      val da = df.select("arr1")
      val ds = createdarr1()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdarr2(): Dataset[Row] = {
      var ds = Seq[arr2class]()
      for (i <- 0 to 99) {
        var arr = new Array[java.lang.Double](100)
        for (j <- 0 to 99) {
          if (j == 1) {
            arr(j) = 1.0
          }
          else {
            arr(j) = 0.0
          }
        }
        ds = ds :+ arr2class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparearr2() = {
      val da = df.select("arr2")
      val ds = createdarr2()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdarr3(): Dataset[Row] = {
      var ds = Seq[arr3class]()
      val one: Float = 1
      val zero: Float = 0
      for (i <- 0 to 99) {
        var arr = new Array[java.lang.Float](100)
        for (j <- 0 to 99) {
          if (j == 1) {
            arr(j) = one
          }
          else {
            arr(j) = zero
          }
        }
        ds = ds :+ arr3class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparearr3() = {
      val da = df.select("arr3")
      val ds = createdarr3()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdarr4(): Dataset[Row] = {
      var ds = Seq[arr4class]()
      val one: Byte = 1
      val zero: Byte = 0
      for (i <- 0 to 99) {
        var arr = new Array[java.lang.Byte](100)
        for (j <- 0 to 99) {
          if (j == 1) {
            arr(j) = one
          }
          else {
            arr(j) = zero
          }
        }
        ds = ds :+ arr4class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparearr4() = {
      val da = df.select("arr4")
      val ds = createdarr4()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdarr5(): Dataset[Row] = {
      var ds = Seq[arr5class]()
      for (i <- 0 to 99) {
        var arr = new Array[java.lang.Boolean](100)
        for (j <- 0 to 99) {
          if (j == 1) {
            arr(j) = true
          }
          else {
            arr(j) = false
          }
        }
        ds = ds :+ arr5class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparearr5() = {
      val da = df.select("arr5")
      val ds = createdarr5()
      assertLargeDataFrameEquality(da,ds)
    }

    def createstr() = {
      var ds = Seq[strclass]()
      for (i <- 0 to 99) {
        ds = ds :+ strclass("abc")
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparestr() = {
      val da = df.select("str")
      val ds = createstr()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdmulti1(): Dataset[Row] = {
      var ds = Seq[multi1class]()
      var arr1 = new Array[Array[Array[Integer]]](2)
      var arr21 = new Array[Array[Integer]](2)
      var arr22 = new Array[Array[Integer]](2)
      var arr3 = new Array[Integer](2)
      var arr4 = new Array[Integer](2)
      arr3(0) = 0
      arr3(1) = 0
      arr4(0) = 0
      arr4(1) = 1
      arr21(0) = arr3
      arr21(1) = arr3
      arr22(0) = arr3
      arr22(1) = arr4
      arr1(0) = arr21
      arr1(1) = arr22
      for (i <- 0 to 99) {
        ds = ds :+ multi1class(arr1)
      }

      import spark.implicits._
      ds.toDF()
    }

    def comparemulti1() = {
      val da = df.select("multi1")
      val ds = createdmulti1()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdmulti2(): Dataset[Row] = {
      var ds = Seq[multi2class]()
      var arr1 = new Array[Array[Array[java.lang.Double]]](2)
      var arr21 = new Array[Array[java.lang.Double]](2)
      var arr22 = new Array[Array[java.lang.Double]](2)
      var arr3 = new Array[java.lang.Double](2)
      var arr4 = new Array[java.lang.Double](2)
      arr3(0) = 0.0
      arr3(1) = 0.0
      arr4(0) = 0.0
      arr4(1) = 1.0
      arr21(0) = arr3
      arr21(1) = arr3
      arr22(0) = arr3
      arr22(1) = arr4
      arr1(0) = arr21
      arr1(1) = arr22
      for (i <- 0 to 99) {
        ds = ds :+ multi2class(arr1)
      }

      import spark.implicits._
      ds.toDF()
    }

    def comparemulti2() = {
      val da = df.select("multi2")
      val ds = createdmulti2()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdmulti3(): Dataset[Row] = {
      var ds = Seq[multi3class]()
      var arr1 = new Array[Array[Array[java.lang.Float]]](2)
      var arr21 = new Array[Array[java.lang.Float]](2)
      var arr22 = new Array[Array[java.lang.Float]](2)
      var arr3 = new Array[java.lang.Float](2)
      var arr4 = new Array[java.lang.Float](2)
      val zero: Float = 0
      val one: Float = 1
      arr3(0) = zero
      arr3(1) = zero
      arr4(0) = zero
      arr4(1) = one
      arr21(0) = arr3
      arr21(1) = arr3
      arr22(0) = arr3
      arr22(1) = arr4
      arr1(0) = arr21
      arr1(1) = arr22
      for (i <- 0 to 99) {
        ds = ds :+ multi3class(arr1)
      }

      import spark.implicits._
      ds.toDF()
    }

    def comparemulti3() = {
      val da = df.select("multi3")
      val ds = createdmulti3()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdmulti4(): Dataset[Row] = {
      var ds = Seq[multi4class]()
      var arr1 = new Array[Array[Array[java.lang.Byte]]](2)
      var arr21 = new Array[Array[java.lang.Byte]](2)
      var arr22 = new Array[Array[java.lang.Byte]](2)
      var arr3 = new Array[java.lang.Byte](2)
      var arr4 = new Array[java.lang.Byte](2)
      val zero: Byte = 0
      val one: Byte = 1
      arr3(0) = zero
      arr3(1) = zero
      arr4(0) = zero
      arr4(1) = one
      arr21(0) = arr3
      arr21(1) = arr3
      arr22(0) = arr3
      arr22(1) = arr4
      arr1(0) = arr21
      arr1(1) = arr22
      for (i <- 0 to 99) {
        ds = ds :+ multi4class(arr1)
      }

      import spark.implicits._
      ds.toDF()
    }

    def comparemulti4() = {
      val da = df.select("multi4")
      val ds = createdmulti4()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdmulti5(): Dataset[Row] = {
      var ds = Seq[multi5class]()
      var arr1 = new Array[Array[Array[java.lang.Boolean]]](2)
      var arr21 = new Array[Array[java.lang.Boolean]](2)
      var arr22 = new Array[Array[java.lang.Boolean]](2)
      var arr3 = new Array[java.lang.Boolean](2)
      var arr4 = new Array[java.lang.Boolean](2)
      arr3(0) = false
      arr3(1) = false
      arr4(0) = false
      arr4(1) = true
      arr21(0) = arr3
      arr21(1) = arr3
      arr22(0) = arr3
      arr22(1) = arr4
      arr1(0) = arr21
      arr1(1) = arr22
      for (i <- 0 to 99) {
        ds = ds :+ multi5class(arr1)
      }

      import spark.implicits._
      ds.toDF()
    }

    def comparemulti5() = {
      val da = df.select("multi5")
      val ds = createdmulti5()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdn(): Dataset[Row] = {
      var ds = Seq[nclass]()
      for (i <- 0 to 99) {
        ds = ds :+ nclass(i)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparen() = {
      val da = df.select("n")
      val ds = createdn()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdvarr1(): Dataset[Row] = {
      var ds = Seq(varr1class(Array()))
      for (i <- 0 to 98) {
        var c = 0
        var finalarr = new Array[Integer](10000) //Not set to actual upper limit
        for (j <- 0 to i) {
          finalarr(c) = j
          c = c + 1
        }
        var arr = new Array[Integer](c)
        for (j <- 0 to c - 1) {
          arr(j) = finalarr(j)
        }
        ds = ds :+ varr1class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparevarr1() = {
      val da = df.select("varr1")
      val ds = createdvarr1()
      assertLargeDataFrameEquality(da,ds)
    }

    def createdvarr2(): Dataset[Row] = {
      var ds = Seq(varr2class(Array()))
      for (i <- 0 to 98) {
        var c = 0
        var a = 0.0
        var finalarr = new Array[Double](10000) //Not set to actual upper limit
        for (j <- 0 to i) {
          finalarr(c) = a
          c = c + 1
          a = a + 1.0
        }
        var arr = new Array[java.lang.Double](c)
        for (j <- 0 to c - 1) {
          arr(j) = finalarr(j)
        }
        ds = ds :+ varr2class(arr)
      }
      import spark.implicits._
      ds.toDF()
    }

    def comparevarr2() = {
      val da = df.select("varr2")
      val ds = createdvarr2()
      assertLargeDataFrameEquality(da,ds)
    }

    comparemyintvector
    comparemyvector2
    comparevofvofDouble
    //compareMuons
    comparea
    compareb
    comparec
    compared
    comparef
    comparearr1
    comparearr2
    comparearr3
    comparearr4
    comparearr5
    comparemulti1
    comparemulti2
    comparemulti3
    comparemulti4
    comparemulti5
    comparen
    comparestr
    comparevarr1
    comparevarr2

    spark.stop()
  }
}




