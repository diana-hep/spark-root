package org.dianahep.sparkroot.ast

import org.dianahep.root4j.interfaces._
import org.apache.spark.sql.types._

/**
 *  Base Class for all the types
 */
abstract class SRType
{
  def getIterator(b: TBranch): BranchIterator[Any]
  def toSparkType: DataType
}

//  null
case object SRNull extends SRType
{
  override def getIterator(b: TBranch) = null
  override def toSparkType: DataType = NullType
}

/**
 *  Base Class for all the Basic Types. C Struct is also a derivative of a basic type
 */
abstract class SRBasicType extends SRType
{
  override def getIterator(b: TBranch): BasicBranchIterator = 
    BasicBranchIterator(b)
}
case object SRStringType extends SRBasicType
{
  override def toSparkType: DataType = StringType
}
case object SRByteType extends SRBasicType
{
  override def toSparkType: DataType = ByteType
}
case object SRShortType extends SRBasicType
{
  override def toSparkType: DataType = ShortType
}
case object SRIntegerType extends SRBasicType
{
  override def toSparkType: DataType = IntegerType
}
case object SRFloatType extends SRBasicType
{
  override def toSparkType: DataType = FloatType
}
case object SRDoubleType extends SRBasicType
{
  override def toSparkType: DataType = DoubleType
}
case object SRLongType extends SRBasicType
{
  override def toSparkType: DataType = LongType
}
case object SRBooleanType extends SRBasicType
{
  override def toSparkType: DataType = BooleanType
}

case class SRArrayType(t: SRType, n: Int) extends SRBasicType
{
  private def iterate(dimsToGo: Int): ArrayType = 
    if (dimsToGo==1) ArrayType(t.toSparkType)
    else ArrayType(iterate(dimsToGo-1))
  private val expandedType = iterate(n)

  override def toSparkType: DataType = expandedType
}

case class SRStructType(memberNamesTypes: Seq[(String, SRType)]) extends SRType
{
  override def toSparkType: DataType = StructType(
    for (p <- memberNamesTypes) yield StructField(p._1, p._2.toSparkType)
  )
  override def getIterator(branch: TBranch) = StructBranchIterator(branch)
}

/*
 * Case Classes to represent the STL Collection Types
 */
/*case class SRSTLVector() extends SRType
{
  override def toSparkType: DataType
  override def getIterator(branch: TBranch) = 
}*/
/*
case class SRSTLList() extends SRType
{
  override def toSparkType: DataType
  override def getIterator(branch: TBranch) = 
}
case class SRSTLDeque() extends SRType
{
  override def toSparkType: DataType
  override def getIterator(branch: TBranch) = 
}
case class SRSTLMap() extends SRType
{
  override def toSparkType: DataType
  override def getIterator(branch: TBranch) = 
}*/
