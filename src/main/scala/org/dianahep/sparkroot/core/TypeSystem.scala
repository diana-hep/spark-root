package org.dianahep.sparkroot.core

import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.dianahep.root4j.core.RootInput
import org.dianahep.root4j.interfaces._

/**
 * @author Viktor Khristenko
 * @version $Id: TypeSystem.scala
 *
 * Defines a TypeSystem used by Spark-Root
 */

trait SRTypeTag;
case object SRRootType extends SRTypeTag;
case object SRCollectionType extends SRTypeTag;
case object SRCompositeType extends SRTypeTag;

abstract class SRType(val name: String) {
  // reuse the same buffer
  def read(b: RootInput): Any
  // use you own buffer
  def read: Any
  // for iteration
  def hasNext: Boolean
  // to convert to spark
  val toSparkType: DataType

  protected var entry = 0L
}
abstract class SRSimpleType(name: String, b: TBranch, l: TLeaf) extends SRType(name) {
}
abstract class SRCollection(name: String, isTop: Boolean) extends SRType(name);
case object SRNull extends SRType("Null") {
  override def read(b: RootInput) = null
  override def read = null
  override def hasNext = false
  override val toSparkType = NullType
}

case class SRRoot(override val name: String, var entries: Long, types: Seq[SRType]) extends SRType(name) {
  override def read(b: RootInput) = null
  override def read = {
    entries-=1;
    Row.fromSeq(for (t <- types) yield t.read)
  }
  override def hasNext = entries>0

  override val toSparkType = StructType(
    for (t <- types) yield StructField(t.name, t.toSparkType)
  )
}
case class SREmptyRoot(override val name: String, var entries: Long) extends SRType(name) {
  override def read(b: RootInput) = null
  override def read = {
    entries-=1;Row()
  }
  override def hasNext = entries>0
  override val toSparkType = StructType(Seq())
}

case class SRString(override val name: String, b: TBranch, l: TLeaf) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L;buffer.readString}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readString
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = StringType
}

case class SRShort(override val name: String, b: TBranch, l: TLeaf) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L;buffer.readShort}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readShort
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = ShortType
}

case class SRBoolean(override val name: String, b: TBranch, l: TLeaf) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L;buffer.readBoolean}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readBoolean
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = BooleanType
}

case class SRLong(override val name: String, b: TBranch, l: TLeaf) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L;buffer.readLong}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readLong
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = LongType
}

case class SRDouble(override val name: String, b: TBranch, l: TLeaf)
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L; buffer.readDouble}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readDouble
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = DoubleType
}

case class SRByte(override val name: String, b: TBranch, l: TLeaf) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L;buffer.readByte}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readByte
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = ByteType
}

case class SRInt(override val name: String, b: TBranch, l: TLeaf) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L;buffer.readInt}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readInt
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = IntegerType
}
case class SRFloat(override val name: String, b: TBranch, l: TLeaf) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {entry+=1L;buffer.readFloat}
  override def read = {
    val buffer = b.setPosition(l, entry)
    val data = buffer.readFloat
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = FloatType
}

case class SRArray(override val name: String, b: TBranch, l:TLeaf, t: SRType, n: Int) 
  extends SRSimpleType(name, b, l) {
  override def read(buffer: RootInput) = {
    val data = 
      if (n == -1) 
        for (i <- 0 until l.getLeafCount.getWrappedValue(entry).asInstanceOf[Integer]) 
      yield t.read(buffer)
    else for (i <- 0 until n) yield t.read(buffer)
    entry+=1L
    data
  }
  override def read = {
    // array is read contiguously - no buffer reassigning
    val buffer = b.setPosition(l, entry)
    val data = 
      if (n == -1) 
        for (i <- 0 until l.getLeafCount.getWrappedValue(entry).asInstanceOf[Integer])
        yield t.read(buffer)
      else for (i <- 0 until n) yield t.read(buffer)
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = ArrayType(t.toSparkType)
}

/**
 * STL Vector Representation
 */
case class SRVector(override val name: String, b: TBranchElement, 
  t: SRType, split: Boolean, isTop: Boolean) extends SRCollection(name, isTop) {
  /** 
   * reading by assigning the buffer
   *
   * @return the vector of SRTyp
   */
  override def read = 
    if (split) {
      // vector collection is split
      null
    }
    else {
      // vector collection is not split and we assign the buffer =>
      // we are the top level of vector nestedness 
      // 1. assign the buffer
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeaf], entry)
      // 2. read version and checksum - Short and Int
      buffer.readShort; buffer.readInt
      val size = buffer.readInt
      for (i <- 0 until size) yield t.read(buffer)
    }
  /**
  * reading by reusing the buffer passed
  */
  override def read(buffer: RootInput) = 
    if (split) {
      null
    }
    else {
      // vector collection inside of something as the buffer has been passed
      // for the vector of top level read the version first
      // 1. read the size
      // 2. pass the buffer downstream for reading. 
      if (isTop) { buffer.readShort; buffer.readInt}

      val size = buffer.readInt
      for (i <- 0 until size) yield t.read(buffer)
    }
  override def hasNext = entry<b.getEntries
  override val toSparkType = ArrayType(t.toSparkType)
/*  override def read = {
    // overhead
    // buffer.readShort

    // size + data
    //val size = buffer.readInteger
    val size = 5
    for (x <- 0 until size) yield t.read
  }*/
}

case class SRComposite(b: TBranch, members: Seq[SRType], split: Boolean) extends SRType(b.getName) {
  // composite type always occupies a separate branch....
  override val name: String = b.getName

  /**
   * reading by assigning the buffer.
   * 1. For a class that is split =>
   * - means that all the members are separate and do not need the buffer.
   * - means that we do not need to explicitly read short 3 times before getting ahold
   *   of data
   * 2. For a class that is not split =>
   * - means that members will be contiguously stored and require the buffer to be passed
   *   downstream
   * - m
   */
  override def read = 
    if (split) {
      // split class
      entry+=1L
      Row.fromSeq(for (m <- members) yield m.read)
    }
    else {
      // unpslitted class
      // 1. get the buffer
      // 2. read the header - 3xshort
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeaf], entry)
      buffer.readShort; buffer.readShort; buffer.readShort
      // 3. read members
      val data = for (m <- members) yield m.read(buffer)
      entry+=1L
      Row.fromSeq(data)
    }

  /**
   * reading by reusing the buffer
   */
  override def read(buffer: RootInput) = {
    entry+=1L
    Row.fromSeq(for (m <- members) yield m.read(buffer))
  }

  override def hasNext = entry<b.getEntries
  override val toSparkType = StructType(
    for (t <- members) yield StructField(t.name, t.toSparkType)
  )
}
