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

abstract class SRSimpleType(name: String, b: TBranch, l: TLeaf) extends SRType(name);
abstract class SRCollection(name: String, isTop: Boolean) extends SRType(name) {
  protected val kMemberWiseStreaming = 0x4000
}

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
case class SREmptyRoot(override val name: String, var entries: Long) 
  extends SRType(name) {
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

/*
case class SRMap(override val name: String, b: TBranchElement,
keyType: SRType, valueType: SRType, split: Boolean, isTop: Boolean) 
extends SRCollection(name, isTop) {
  // aux constructor where key is the members(0) and members(1) is the value
  def this(name: String, b: TBranchElement, types: SRComposite, split: Boolean,
    isTop: Boolean) = this(name, b, types.members(0), types.members(1), split, isTop)

  override def read = 
    if (split) {
      null
    }
    else {
      // collection is not split and we assign the buffer =>
      // we are the top level of collection nestedness 
      // 1. assign the buffer
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeaf], entry)
      // 2. read version and checksum - Short and Int
      buffer.readShort; buffer.readInt
      val size = buffer.readInt
      (for (i <- 0 until size) yield (keyType.read(buffer), valueType.read(buffer))).toMap
    }

  override def read(buffer: RootInput) = 
    if (split) {
      null
    }
    else {
      // map collection inside of something as the buffer has been passed
      // for the top level of nestedness read the version
      // 1. read the size
      // 2. pass the buffer downstream for reading. 
      if (isTop) { buffer.readShort; buffer.readInt}
      val size = buffer.readInt
      (for (i <- 0 until size) yield (keyType.read(buffer), valueType.read(buffer))).toMap
    }

  override def hasNext = entry<b.getEntries
  override val toSparkType = MapType(keyType.toSparkType, valueType.toSparkType)
}
*/

/**
 * STL Vector Representation
 */
case class SRMap(
  override val name: String, // actual name if branch is null
  b: TBranchElement, // branch to read from... 
  keyType: SRType, // key type
  valueType: SRType, // value type
  split: Boolean, // does it have subbranches
  isTop: Boolean // is this map nested in another collection?
  ) extends SRCollection(name, isTop) {
  // aux constructor where key is the members(0) and members(1) is the value
  def this(name: String, b: TBranchElement, types: SRComposite, split: Boolean,
    isTop: Boolean) = this(name, b, types.members(0), types.members(1), split, isTop)

  /** 
   * reading by assigning the buffer - we own the branch
   * @return the map of SRType
   */
  override def read = 
    if (split) {
      // current collection has subbranches - this must be an STL node
      // don't check for isTop - a split one must be top
      val leaf = b.getLeaves.get(0).asInstanceOf[TLeaf]
      val buffer = b.setPosition(leaf, entry)

      // for a split collection - size is in the collection leaf node
      val size = buffer.readInt

      // check for streaming type - object wise or memberwise
      val version = b.getClassVersion
      if ((version & kMemberWiseStreaming) > 0) {
        null
        /*
        // we need to read the version of the vector member
        val memberVersion = buffer.readShort
        // if 0 - read checksum
        if (memberVersion == 0) buffer.readInt

        // we get Seq(f1[size], f2[size], ..., fN[size])
        // we just have to transpose it
        entry += 1L;
        for (x <- composite.members)
          yield for (i <- 0 until size) yield x.read).transpose
          */
      }
      else {
        // object wise streaming
        entry += 1L;
        (for (i <- 0 until size) yield (keyType.read, valueType.read)).toMap
      }
    }
    else {
      // this map collection does not have subbranches.
      // we are the top level of collection nestedness - 
      //  nested collections will always pass the buffer
      // composite can call w/o passing the buffer.
      //
      // 1. assign the buffer
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeaf], entry)

      // read the byte count, version
      val byteCount = buffer.readInt
      val version = buffer.readShort

      // check if the version has BIT(14) on - memberwise streaming
      if ((version & kMemberWiseStreaming)>0) {
        null
        /*
        // memberwise streaming
        // assume we have some composite inside
        val composite = t.asInstanceOf[SRComposite]

        // member Version
        val memberVersion = buffer.readShort
        // if 0 - read checksum
        if (memberVersion == 0) buffer.readInt

        // now read the size of the vector
        val size = buffer.readInt

        // have to transpose
        entry += 1L;
        (for (x <- composite.members)
          yield for (i <- 0 until size) yield x.read(buffer)).transpose
        */
      }
      else {
        // get the size
        val size = buffer.readInt

        entry += 1L;
        (for (i <- 0 until size) yield (keyType.read(buffer), 
          valueType.read(buffer))).toMap
      }
    }

  /**
  * reading by reusing the buffer passed
  */
  override def read(buffer: RootInput) = 
    if (split) {
      // there are subbranches and we are passed a buffer
      // TODO: Do we have such cases???
      null
    }
    else {
      // collection inside of something as the buffer has been passed
      // for the collection of top level read the version first
      // NOTE: we must know if this is the top collection or not.
      // -> If it is, then we do read the version and check the streaming type
      // -> else, this is a nested collection - we do not read the header and assume
      //  that reading is done object-wise
      //
      // 1. read the size
      // 2. pass the buffer downstream for reading. 
      if (isTop) { 
        val byteCount = buffer.readInt
        val version = buffer.readShort

        if ((version & kMemberWiseStreaming)>0) {
          /*
          // memberwise streaming
          // assume we have a composite
          val composite = t.asInstanceOf[SRComposite]

          // memeberVersion
          val memberVersion = buffer.readShort
          // if 0 - read checksum
          if (memberVersion == 0) buffer.readInt

          // size 
          val size = buffer.readInt

          // have to transpose
          entry += 1L;
          (for (x <- composite.members)
            yield for (i <- 0 until size) yield x.read(buffer)).transpose
          */
        }
        else {
          // object wise streaming
          val size = buffer.readInt
          entry += 1L;
          (for (i <- 0 until size) yield (keyType.read(buffer), 
            valueType.read(buffer))).toMap
        }
      }
      else {
        // just read the size and object-wise raeding of all elements
        val size = buffer.readInt
        entry += 1L;
        (for (i <- 0 until size) yield (keyType.read(buffer),
          valueType.read(buffer))).toMap
      }
    }

  override def hasNext = entry<b.getEntries
  override val toSparkType = MapType(keyType.toSparkType, valueType.toSparkType)
}

/**
 * STL Vector Representation
 */
case class SRVector(
  override val name: String, // actual name if branch is null
  b: TBranchElement, // branch to read from... 
  t: SRType, // value member type
  split: Boolean, // does it have subbranches
  isTop: Boolean // is this vector nested in another collection?
  ) extends SRCollection(name, isTop) {
  /** 
   * reading by assigning the buffer - we own the branch
   * @return the vector of SRType
   */
  override def read = 
    if (split) {
      // current collection has subbranches - this must be an STL node
      // don't check for isTop - a split one must be top
      val leaf = b.getLeaves.get(0).asInstanceOf[TLeaf]
      val buffer = b.setPosition(leaf, entry)

      // for a split collection - size is in the collection leaf node
      val size = buffer.readInt

      // check for streaming type - object wise or memberwise
      val version = b.getClassVersion
      if ((version & kMemberWiseStreaming) > 0) {
        // memberwise streaming, safely case our composite
        val composite = t.asInstanceOf[SRComposite]

        // we need to read the version of the vector member
        val memberVersion = buffer.readShort
        // if 0 - read checksum
        if (memberVersion == 0) buffer.readInt

        // we get Seq(f1[size], f2[size], ..., fN[size])
        // we just have to transpose it
        entry += 1L;
        (for (x <- composite.members)
          yield {for (i <- 0 until size) yield x.read}).transpose
      }
      else {
        // object wise streaming
        entry += 1L;
        for (i <- 0 until size) yield t.read
      }
    }
    else {
      // this vector collection does not have subbranches.
      // we are the top level of vector nestedness - 
      //  nested collections will always pass the buffer
      // composite can call w/o passing the buffer.
      //
      // 1. assign the buffer
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeaf], entry)

      // read the byte count, version
      val byteCount = buffer.readInt
      val version = buffer.readShort

      // check if the version has 14th bit on - memberwise streaming
      if ((version & kMemberWiseStreaming) > 0) {
        // memberwise streaming
        // assume we have some composite inside
        val composite = t.asInstanceOf[SRComposite]

        // member Version
        val memberVersion = buffer.readShort
        // if 0 - read checksum
        if (memberVersion == 0) buffer.readInt

        // now read the size of the vector
        val size = buffer.readInt

        // have to transpose
        entry += 1L;
        (for (x <- composite.members)
          yield {for (i <- 0 until size) yield x.read(buffer)}).transpose
      }
      else {
        // get the size
        val size = buffer.readInt

        entry += 1L;
        for (i <- 0 until size) yield t.read(buffer)
      }
    }

  /**
  * reading by reusing the buffer passed
  */
  override def read(buffer: RootInput) = 
    if (split) {
      // there are subbranches and we are passed a buffer
      // TODO: Do we have such cases???
      null
    }
    else {
      // vector collection inside of something as the buffer has been passed
      // for the vector of top level read the version first
      // NOTE: we must know if this is the top collection or not.
      // -> If it is, then we do read the version and check the streaming type
      // -> else, this is a nested collection - we do not read the header and assume
      //  that reading is done object-wise
      //
      // 1. read the size
      // 2. pass the buffer downstream for reading. 
      if (isTop) { 
        val byteCount = buffer.readInt
        val version = buffer.readShort

        if ((version & kMemberWiseStreaming) > 0) {
          // memberwise streaming
          // assume we have a composite
          val composite = t.asInstanceOf[SRComposite]

          // memeberVersion
          val memberVersion = buffer.readShort
          // if 0 - read checksum
          if (memberVersion == 0) buffer.readInt

          // size 
          val size = buffer.readInt

          // have to transpose
          entry += 1L;
          (for (x <- composite.members)
            yield {for (i <- 0 until size) yield x.read(buffer)}).transpose
        }
        else {
          // object wise streaming
          val size = buffer.readInt
          entry += 1L;
          for (i <- 0 until size) yield t.read(buffer)
        }
      }
      else {
        // just read the size and object-wise raeding of all elements
        val size = buffer.readInt
        entry += 1L;
        for (i <- 0 until size) yield t.read(buffer)
      }
    }

  override def hasNext = entry<b.getEntries
  override val toSparkType = ArrayType(t.toSparkType)
}

/**
 * Composite (non-iterable class) representation
 */
case class SRComposite(
  override val name: String, // name
  b: TBranch, // branch
  members: Seq[SRType], // fields
  split: Boolean, // is it split?
  isTop: Boolean // is it a top level branch?
  ) extends SRType(name) {
  /**
   * reading by assigning the buffer.
   * 1. For a class that is split =>
   * - means that all the members are separate and do not need the buffer.
   * 2. For a class that is not split =>
   * - means that members will be contiguously stored and require the buffer to be passed
   *   downstream
   * - m
   */
  override def read = 
    if (split) {
      // split class -- just pass the call to members
      // do not have to read the header information
      entry+=1L
      Row.fromSeq(for (m <- members) yield m.read)
    }
    else {
      // composite is not split into subbranches for members
      // get the buffer
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeaf], entry)

      // check if this branch is top level or not
      if (isTop) {
        // top level type-branch
        // do not read the header information
        entry+=1L
        // pass the buffer downstream
        Row.fromSeq(for (m <- members) yield m.read(buffer))
      }
      else {
        // not a top level type-branch
        // we have to read the header
        val byteCount = buffer.readInt
        val version = buffer.readShort
        // if 0 - read checksum
        if (version==0) buffer.readInt
        
        entry += 1L
        Row.fromSeq(for (m <- members) yield m.read(buffer))
      }
    }

  /**
   * reading by reusing the buffer
   */
  override def read(buffer: RootInput) = {
    // not a top branch
    // can not be split - composite that receive the buffer are contiguous

    entry+=1L
    
    // read the header
    val byteCount = buffer.readInt
    val version = buffer.readShort
    if (version == 0) buffer.readInt

    // read
    Row.fromSeq(for (m <- members) yield m.read(buffer))
  }

  override def hasNext = entry<b.getEntries
  override val toSparkType = StructType(
    for (t <- members) yield StructField(t.name, t.toSparkType)
  )
}
