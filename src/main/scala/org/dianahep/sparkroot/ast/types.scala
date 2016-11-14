package org.dianahep.sparkroot.ast

import org.dianahep.root4j.interfaces._
import org.apache.spark.sql.types._

abstract class SRType
{
  def getIterator(b: TBranch): BranchIterator[Any]
  def toSparkType: DataType
}
case object SRNull extends SRType
{
  def getIterator(b: TBranch) = null
  override def toSparkType: DataType = NullType
}
case object SRStringType extends SRType
{
  override def getIterator(b: TBranch) = null
  override def toSparkType: DataType = StringType
}
case object SRByteType extends SRType
{
  override def getIterator(b: TBranch) = ByteBranchIterator(b)
  override def toSparkType: DataType = ByteType
}
case object SRShortType extends SRType
{
  override def getIterator(b: TBranch) = ShortBranchIterator(b)
  override def toSparkType: DataType = ShortType
}
case object SRIntegerType extends SRType
{
  override def getIterator(b: TBranch) = IntBranchIterator(b)
  override def toSparkType: DataType = IntegerType
}
case object SRFloatType extends SRType
{
  override def getIterator(b: TBranch) = FloatBranchIterator(b)
  override def toSparkType: DataType = FloatType
}
case object SRDoubleType extends SRType
{
  override def getIterator(b: TBranch) = DoubleBranchIterator(b)
  override def toSparkType: DataType = DoubleType
}
case object SRLongType extends SRType
{
  override def getIterator(b: TBranch) = LongBranchIterator(b)
  override def toSparkType: DataType = LongType
}
case object SRBooleanType extends SRType
{
  override def getIterator(b: TBranch) = BoolBranchIterator(b)
  override def toSparkType: DataType = BooleanType
}

case class SRArrayType(t: SRType) extends SRType
{
  override def getIterator(b: TBranch) = t match {
    case SRByteType => ByteFixedSeqBranchIterator(b)
    case SRIntegerType => IntFixedSeqBranchIterator(b)
    case SRFloatType => FloatFixedSeqBranchIterator(b)
    case SRDoubleType => DoubleFixedSeqBranchIterator(b)
    case SRBooleanType => BoolFixedSeqBranchIterator(b)
    case _ => null
  }
  override def toSparkType: DataType = ArrayType(t.toSparkType)
}
case class SRMultiArrayType(t: SRType, dims: Seq[Int]) extends SRType
{
  override def getIterator(b: TBranch) = MultiFixedSeqBranchIterator(
    SRArrayType(t).getIterator(b).asInstanceOf[FixedSeqBranchIterator[Any]], b, dims)
  override def toSparkType: DataType = {
    def iterate(d: Seq[Int]): DataType = d match {
      case xs :: rest => ArrayType(iterate(rest))
      case Nil => t.toSparkType
    }
    iterate(dims)
  }
}

/**
 * Represents a struct type
 * TODO: no nesting of structs
 */
case class SRStructType(memberNamesTypes: Seq[(String, SRType)]) extends SRType
{
  override def getIterator(b: TBranch) = StructBranchIterator(b, memberNamesTypes)
  override def toSparkType: DataType = StructType(
    for (p <- memberNamesTypes) yield StructField(p._1, p._2.toSparkType)
  )
}
