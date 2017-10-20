package org.dianahep.sparkroot.experimental

// spark
import org.apache.spark.sql.types._

// scala std
import scala.collection.mutable.Queue

// spark-root
import org.dianahep.sparkroot.experimental.core._

package object codegen {
  // generate a string that contains Case Class definitions for all of the 
  // types in the StructType
  implicit class SchemaCodeGen(schema: StructType) {
    def codeGen(topName: String = "Event"): Queue[String] = {
      val queue = Queue[String]()
      iterate(schema, topName, queue)
      queue
    }

    private var counter = 0
  
    private def getType(t: DataType, queue: Queue[String]): String = t match {
      case x: StructType => {
        val num = counter;
        // increment the total counter
        counter+=1;
        iterate(x, s"Record${num}", queue)
        s"Record${num}"
      }
      case x: ArrayType => {
        s"Seq[${getType(x.elementType, queue)}]"
      }
      case x: MapType => {
        s"Map[${getType(x.keyType, queue)}, ${getType(x.valueType, queue)}]"
      }
      case _:ByteType => "Byte"
      case _:ShortType => "Short"
      case _:IntegerType => "Int"
      case _:LongType => "Long"
      case _:FloatType => "Float"
      case _:DoubleType => "Double"
      case _:DecimalType => "java.math.BigDecimal"
      case _:StringType => "String"
      case _:BinaryType => "Array[Byte]"
      case _:BooleanType => "Boolean"
      case _:TimestampType => "java.sql.Timestamp"
      case _ => "String"
    //  case _:DateType => "java.sql.Date"
    }

    private def iterate(record: StructType, name: String, queue: Queue[String]): Unit = {
      val fieldsStr = record.fields.map({
        case field => s"|    ${field.name} : ${getType(field.dataType, queue)}"
      }).mkString(",\n")

      // you have enqueue a class definition only after fields are enqueued
      queue += s"""
      |case class ${name} (
      ${fieldsStr}
      |)
      |""".stripMargin
    }
  }
}
