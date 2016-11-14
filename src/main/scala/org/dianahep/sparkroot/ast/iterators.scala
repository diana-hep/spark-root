package org.dianahep.sparkroot.ast

import org.dianahep.root4j.core.RootInput
import org.dianahep.root4j._
import org.dianahep.root4j.interfaces._

import scala.reflect.runtime.universe._

/**
 *  TODO: 
 *  1. Separate the reading part from the Iterator Abstraction
 *  2. Optimize the reading of Fixed Sized Types
 *    currently read them one by one => read a chunk and split it.
 */

/**
 *  Abstract Class for single-valued iterators
 */
trait Reader[+T] {
  def readOne(rootInput: RootInput): T
}
abstract class BranchIterator[+T : TypeTag](branch: TBranch) extends Iterator[T]
{
  //  leaf - currently just have 1 leaf per branch - are there other situations???
  protected val leaf = branch.getLeaves.get(0).asInstanceOf[TLeaf]
  //  array of entry numbers that are first in each basket
  protected val startingEntries = branch.getBasketEntry
  //  get the # bytes for a given leaf type
  protected val typeSize: Int
  //  basket #
  protected var basket = 0
  //  entry # - a la event #
  protected var entry = 0L
  //  basically if it is, we will have to switch the basket
  protected def lastInBasket = entry == startingEntries(basket + 1) - 1
  
  //  these 3 to be overriden
//  def makeOutput(size: Int): T
//  protected def readOne(rootInput: RootInput): T

  //  expose only these guys
  def hasNext: Boolean = basket != startingEntries.size-1
  def next: T
}

/**
 * Multi Leaf Iterator - basically a struct of fields
 * TODO:
 *  1. Currently cannot nest the structs in here for leaves
 */
case class StructBranchIterator(branch: TBranch, leafNamesTypes: Seq[(String, SRType)]) 
  extends BranchIterator[Seq[Any]](branch)
{
  override val typeSize = 0

  //  we need the whole list of those leaves 
  protected val leaves = for (i <- 0 until branch.getLeaves.size; 
    l=branch.getLeaves.get(i).asInstanceOf[TLeaf]
  ) yield l

  //  
  protected def readLeaf(rootInput: RootInput, p: ((String, SRType), TLeaf)) = {
    // now read either 1 value or an array of values
    if (p._2.getLen==1) // 1 value
    {
      p._1._2.getIterator(branch).asInstanceOf[BasicBranchIterator[Any]].readOne(rootInput)
    }
    else { // array of values
      val iter = p._1._2.getIterator(branch).asInstanceOf[MultiFixedSeqBranchIterator]
      iter.dimensionalize(
        for (i <- 0 until p._2.getLen) yield iter.readOne(rootInput)
      )
    }
  }

  def next = {
    // switch the basket
    if (lastInBasket) 
      basket+=1

    //  iterate over all the leaves and read them one by one
    val rootInput = branch.setPosition(leaf, entry)
    val data = for (p <- leafNamesTypes.zip(leaves)) yield readLeaf(rootInput, p)
    entry+=1L
    data
  }
}

/**
 * Abstract Iterator over a branch that stores Basic Type - no arrays 
 */
abstract class BasicBranchIterator[+T: TypeTag](branch: TBranch)
  extends BranchIterator[T](branch) with Reader[T]
{
  override protected val typeSize = leaf.getLenType()
  def next = 
  {
    //  switch the basket
    if (lastInBasket)
      basket+=1

    //  set position to the right byte and read it
    val rootInput = branch.setPosition(leaf, entry)
    entry+=1L
    readOne(rootInput)
  }
}
case class BoolBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Boolean](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readBoolean
}
case class ByteBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Byte](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readByte
}
case class CharBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Char](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readChar
}
case class ShortBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Short](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readShort
}
case class IntBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Int](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readInt
}
case class LongBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Long](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readLong
}
case class FloatBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Float](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readFloat
}
case class DoubleBranchIterator(branch: TBranch) 
  extends BasicBranchIterator[Double](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readDouble
}

/**
 *  Abstract Class Iterator for Fixed Type Size Arrays
 */
abstract class FixedSeqBranchIterator[+T: TypeTag](branch: TBranch)
  extends BranchIterator[Seq[T]](branch) with Reader[T]
{
  //  this is the fixed size type
  override protected val typeSize = leaf.getLenType()
  //  have to kill this guy

  def next: Seq[T] = 
  {
    val endPosition = 
      if (lastInBasket)
      {
        val rootInput = branch.setPosition(leaf, entry)
        basket+=1
        rootInput.getLast
      }
      else branch.setPosition(leaf, entry+1).getPosition

    //  set the position to start reading from
    val rootInput = branch.setPosition(leaf, entry)
    val n = (endPosition - rootInput.getPosition).toInt/typeSize
    entry+=1
    for (i <- 0 until n) yield readOne(rootInput)
  }
}

/**
 * Multidimensional Fixed Sequence Iterator
 */
case class MultiFixedSeqBranchIterator(fixedIterator: FixedSeqBranchIterator[Any],
  branch: TBranch, dims: Seq[Int]) extends FixedSeqBranchIterator[Any](branch)
{
  def dimensionalize(x: Seq[Any]): Seq[Any] = {
    def iterate(seq: Seq[Any], d: Seq[Int]): Seq[Any] = d match {
      case xs :: rest => {
        val it = seq.grouped(xs)
        for (xxx <- it.toList) yield iterate(xxx, rest)
      }
      case Nil => seq
    }
    
    if (dims.length>1)
      iterate(x, dims.dropRight(1)) // we have to drop the last dimension!
    else
      x
  }

  // pass the calls to the fixed one but then arrange the results!
  override def next: Seq[Any] = dimensionalize(fixedIterator.next)
  override def readOne(rootInput: RootInput) = fixedIterator.readOne(rootInput)
}


/**
 * Fixed Sequence Iterators - 1 dimensional
 */
case class BoolFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Boolean](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readBoolean
}
case class ByteFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Byte](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readByte
}
case class CharFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Char](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readChar
}
case class ShortFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Short](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readShort
}
case class IntFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Int](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readInt
}
case class LongFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Long](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readLong
}
case class FloatFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Float](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readFloat
}
case class DoubleFixedSeqBranchIterator(branch: TBranch) 
  extends FixedSeqBranchIterator[Double](branch)
{
  override def readOne(rootInput: RootInput) = rootInput.readDouble
}
