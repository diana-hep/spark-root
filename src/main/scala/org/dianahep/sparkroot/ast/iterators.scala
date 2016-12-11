package org.dianahep.sparkroot.ast

import org.dianahep.root4j.core.RootInput
import org.dianahep.root4j._
import org.dianahep.root4j.interfaces._

import scala.reflect.runtime.universe._

/**
 * Main Abstraction for iterating a over a branch
 */
abstract class BranchIterator[+T: TypeTag](branch: TBranch) extends Iterator[T]
{
  // array of entry numbers that are the first ones in each basket
  protected val startingEntries = branch.getBasketEntry
  // basket #
  protected var basket = 0
  // entry # - a la event #
  protected var entry = 0L
  // switch the basket if this is the last entry in the current one
  protected def lastInBasket = entry== startingEntries(basket+1)-1

  def hasNext: Boolean = basket != startingEntries.size-1
  def next: T
}

/**
 * Multi leaf branches are represented as structs
 * To avoid dynamic casting (need a return not of Any, but Seq[Any])
 */
case class StructBranchIterator(branch: TBranch) extends BranchIterator[Seq[Any]](branch)
{
  protected val leaves = for (i <- 0 until branch.getLeaves.size; 
    l=branch.getLeaves.get(i).asInstanceOf[TLeaf]) yield l

  def next: Seq[Any] = {
    if (lastInBasket) basket+=1
    val data = for (l <- leaves) yield l.getWrappedValue(entry)
    entry+=1L
    data
  }
}

/**
 * Iterator over any Basic Type
 */
case class BasicBranchIterator(branch: TBranch) extends BranchIterator[Any](branch)
{
  protected val leaf = branch.getLeaves.get(0).asInstanceOf[TLeaf]

  def next = {
    if (lastInBasket) basket+=1
    val data = leaf.getWrappedValue(entry)
    entry+=1L
    data
  }
}
