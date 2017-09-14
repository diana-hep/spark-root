package org.dianahep.sparkroot.experimental.core

import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

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

abstract class SRType(val name: String, val _shouldDrop: Boolean = false) {
  protected lazy val logger = LogManager.getLogger("org.dianahep.sparkroot.core.TypeSystem")
  def debugMe(str: String): Unit

  // reuse the same buffer
  def read(b: RootInput): Any
  // use you own buffer
  def read: Any
  // force to read an array of this - use your own buffer
  def readArray(size: Int): Seq[Any]
  // force to read an array of this - reuse the buffer given
  def readArray(buffer: RootInput, size: Int): Seq[Any]
  // for iteration
  def hasNext: Boolean
  // to convert to spark
  val toSparkType: DataType
  // 
  val toName: String = name.replace('.', '_')
  //val toName: String = name

  val shouldDrop: Boolean = _shouldDrop
  def drop: SRType

  protected var entry = 0L
}

abstract class SRSimpleType(
    name: String, 
    b: TBranch, 
    l: TLeaf,
    override val _shouldDrop: Boolean = false
    ) extends SRType(name, _shouldDrop);
abstract class SRCollection(
    name: String, 
    isTop: Boolean,
    override val _shouldDrop: Boolean = false) extends SRType(name, _shouldDrop) {
  protected val kMemberWiseStreaming = 0x4000
}

case class SRUnknown(
    override val name: String, 
    override val _shouldDrop: Boolean = false) extends SRType(name, _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRUnknown::$name $str")

  override def read(b: RootInput) = { 
    debugMe(s"read(buffer)"); 
    null
  }
  override def read = { 
    debugMe(s"read"); 
    null
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    for (i <- 0 until size) yield null
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    for (i <- 0 until size) yield null
  }
  override def hasNext = false
  override val toSparkType = NullType

  override def drop: SRUnknown(name, true)
}

case class SRNull(
    override val _shouldRop: Boolean = false) extends SRType("Null", _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRNull::no name $str")

  override def read(b: RootInput) = {
    debugMe("read(buffer)"); 
    null
  }
  override def read = { 
    debugMe("read"); 
    null
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    for (i <- 0 until size) yield nulli
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    for (i <- 0 until size) yield null
  }
  override def hasNext = false
  override val toSparkType = NullType

  override def drop = SRNull(true)
}

case class SRRoot(
    override val name: String, 
    var entries: Long, 
    types: Seq[SRType]) extends SRType(name, false) {
  override def debugMe(str: String) = logger.debug(s"SRRoot::$name $str")
  override def read(b: RootInput) = {
    debugMe("read(buffer)")
    null
  }
  override def read = {
    debugMe("read")
    entries-=1L;
    Row.fromSeq(for (t <- types) yield t.read)
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    null
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    null
  }
  override def hasNext = entries>0

  override val toSparkType = StructType(
    for (t <- types) yield StructField(t.toName, t.toSparkType)
  )

  override def drop = this
}

object converter {
  def toSRType(name: String, dataType: DataType): SRType = dataType match {
    case StructType(fields) => SRComposite(name, null, 
      field.map({field => toSRType(field.name, field.dataType)}), false, false)

    case StringType => SRString(name, null, null)
    case ShortType => SRShort(name, null, null)
    case BooleanType => SRBoolean(name, null, null)
    case LongType => SRLong(name, null, null)
    case DoubleType => SRDouble(name, null, null)
    case ByteType => SRByte(name, null, null)
    case IntegerType => SRInt(name, null, null)
    case FloatType => SRFloat(name, null, null)
    case ArrayType(elementType, _) => SRArray(name, null, null, 
      toSRType("element", elementType), 0)
    case MapType(keyType, valueType, _) => SRMap(name, null,
      toSRType("key", keyType), toSRType("value", valueType), false, false)
    case NullType => SRNull
    case _ => SRNull
  }
}

case class SREmptyRoot(
    override val name: String, var entries: Long) 
    extends SRType(name, false) {
  override def debugMe(str: String) = logger.debug(s"SREmptyRoot::$name $str")
  override def read(b: RootInput) = {
    debugMe("read(buffer)")
    null
  }
  override def read = {
    debugMe("read")
    entries-=1L;Row()
  }
  override def readArray(b: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    null
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    null
  }
  override def hasNext = entries>0
  override val toSparkType = StructType(Seq())

  override def drop = this
}

case class SRString(
    override val name: String, b: TBranch, l: TLeaf, val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l, _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRString::$name $str")
  override def read(buffer: RootInput) = {
    debugMe("read(buffer)")
    entry+=1L;
    val r = buffer.readString
    debugMe(s"read String=$r")
    r
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readString
    debugMe(s"read String=$data")
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L;
    for (i <- 0 until size) yield buffer.readString
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readString
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = StringType

  override def drop = SRString(name, b, l, true)
}

case class SRShort(
    override val name: String, b: TBranch, l: TLeaf, 
    override val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l, _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRShort::$name $str")
  override def read(buffer: RootInput) = {
    debugMe(s"read(buffer)")
    entry+=1L;
    val r = buffer.readShort
    debugMe(s"read Short=$r")
    r
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readShort
    debugMe(s"read Short=$data")
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L
    for (i <- 0 until size) yield buffer.readShort
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readShort
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = ShortType

  override def drop = SRShort(name, b, l, true)
}

case class SRBoolean(
    override val name: String, b: TBranch, l: TLeaf, 
    override val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l) {
  override def debugMe(str: String) = logger.debug(s"SRBoolean::$name $str")
  override def read(buffer: RootInput) = {
    debugMe("read(buffer)")
    entry+=1L;buffer.readBoolean
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readBoolean
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L
    for (i <- 0 until size) yield buffer.readBoolean
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readBoolean
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = BooleanType

  override def drop = SRBoolean(name, b, l, true)
}

case class SRLong(
    override val name: String, b: TBranch, l: TLeaf, 
    override val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l, _shuoldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRLong::$name $str")
  override def read(buffer: RootInput) = {
    debugMe("read(buffer)")
    entry+=1L;buffer.readLong
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readLong
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L
    for (i <- 0 until size) yield buffer.readLong
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readLong
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = LongType

  override def drop = SRLong(name, b, l, true)
}

case class SRDouble(
    override val name: String, b: TBranch, l: TLeaf, 
    override val _shouldDrop: Boolean = false)
    extends SRSimpleType(name, b, l, shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRDouble::$name $str")
  override def read(buffer: RootInput) = {
    debugMe("read(buffer)")
    entry+=1L; buffer.readDouble
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readDouble
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L
    for (i <- 0 until size) yield buffer.readDouble
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readDouble
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = DoubleType

  override def drop = SRDouble(name, eb, l, true)
}

case class SRByte(
    override val name: String, b: TBranch, l: TLeaf, 
    override val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l, _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRByte::$name $str")
  override def read(buffer: RootInput) = {
    debugMe("read(buffer)")
    entry+=1L;buffer.readByte
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readByte
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L
    for (i <- 0 until size) yield buffer.readByte
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readByte
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = ByteType

  override def drop = SRByte(name, b, l, true)
}

case class SRInt(
    override val name: String, b: TBranch, l: TLeaf, 
    override val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l, _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRInt::$name $str")
  override def read(buffer: RootInput) = {
    debugMe(s"read(buffer)")
    entry+=1L;
    val r = buffer.readInt
    debugMe(s"read Int=$r")
    r
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readInt
    debugMe(s"read Int=$data")
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L
    for (i <- 0 until size) yield buffer.readInt
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readInt
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = IntegerType

  override def drop = SRInt(name, b, l, true)
}
case class SRFloat(
    override val name: String, b: TBranch, l: TLeaf, 
    override val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l, _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRFloat::$name $str")
  override def read(buffer: RootInput) = {
    debugMe(s"read(buffer)")
    entry+=1L;
    val r = buffer.readFloat
    debugMe(s"read Float=$r")
    r
  }
  override def read = {
    debugMe(s"read")
    val buffer = b.setPosition(l, entry)
    val data = buffer.readFloat
    entry+=1L
    debugMe(s"read Float=$data")
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L
    for (i <- 0 until size) yield buffer.readFloat
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield buffer.readFloat
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = FloatType

  override def drop = SRFloat(name,e b, l, true)
}

case class SRArray(
    override val name: String, b: TBranch, l:TLeaf, 
    t: SRType, n: Int, override val _shouldDrop: Boolean = false) 
    extends SRSimpleType(name, b, l, _shouldDrop) {
  override def debugMe(str: String) = logger.debug(s"SRArray::$name $str")
  override def drop = SRArray(name, b, l, t.drop, n, true)
  override def read(buffer: RootInput) = {
    debugMe("read(buffer)")
    val data = 
      if (n == -1) 
        for (i <- 0 until l.getLeafCount.getWrappedValue(entry).asInstanceOf[Integer]) 
      yield t.read(buffer)
    else for (i <- 0 until n) yield t.read(buffer)
    entry+=1L
    data
  }
  override def read = {
    debugMe("read(buffer)")
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

  /**
   * request explicitly to read the array of the Array Type
   * @return Seq[Array]
   */
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    val data = for (i <- 0 until size) yield {
      if (n == -1)
        for (j <- 0 until l.getLeafCount.getWrappedValue(entry).asInstanceOf[Integer])
          yield t.read(buffer)
      else for (j <- 0 until n) yield t.read(buffer)
    }
    entry += 1L
    data
  }
  
  /**
   * request explicitly to read the array of the Array Type
   * @return Seq[Array]
   */
  override def readArray(size: Int) = {
    debugMe(s"readArray($size)")
    val buffer = b.setPosition(l, entry)
    val data = for (i <- 0 until size) yield {
      if (n == -1)
        for (j <- 0 until l.getLeafCount.getWrappedValue(entry).asInstanceOf[Integer])
          yield t.read(buffer)
      else for (j <- 0 until n) yield t.read(buffer)
    }
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = ArrayType(t.toSparkType)
}

/**
 * STL String Representation - has a header(byteCount, version).
 * also different for array reading
 */
case class SRSTLString(override val name: String, 
    b: TBranch,  // branch
    isTop: Boolean, // is it nested inside some other collection? 
    override val _shouldDrop: Boolean = false)
    extends SRCollection(name, isTop, _shouldDrop) {
  override def drop = SRSTLString(name, b, isTop, true)
  override def debugMe(str: String) = logger.debug(s"SRSTLString::$name $str Event=$entry")
  override def read(buffer: RootInput) = {
    debugMe("read(buffer)")
    entry+=1L;
    // read the byte count and version
    // only if this guy is not nested into a collection himself
    if (isTop) {
      buffer.readInt
      buffer.readShort
    }

    // read the data now
    buffer.readString
  }
  override def read = {
    debugMe("read")
    val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeafElement], entry)
    // read the byte count and version
    // only if this guy is not nested in another collection, however
    // isTop must be true in here!
    if (isTop) {
      buffer.readInt
      buffer.readShort
    }

    // read the data
    val data = buffer.readString
    entry+=1L
    data
  }
  override def readArray(buffer: RootInput, size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    entry+=1L;
    // read tehe byte count and version first
    buffer.readInt
    buffer.readShort

    // read the array of these strings
    for (i <- 0 until size) yield buffer.readString
  }
  override def readArray(size: Int) = {
    debugMe(s"readArray(buffer, $size)")
    val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeafElement], entry)
    // reat the byte count and version first
    buffer.readInt; buffer.readShort

    // read array of strings
    val data = for (i <- 0 until size) yield buffer.readString
    entry+=1L
    data
  }
  override def hasNext = entry<b.getEntries
  override val toSparkType = StringType
}

/**
 * STL Vector Representation
 * TODO: Do we have MemberWise Streaming for a map???
 */
case class SRMultiMap(
    override val name: String, // actual name if branch is null
    b: TBranchElement, // branch to read from... 
    keyType: SRType, // key type
    valueType: SRType, // value type
    split: Boolean, // does it have subbranches
    isTop: Boolean, // is this map nested in another collection?
    override val _shouldDrop: Boolean = false
    ) extends SRCollection(name, isTop, _shouldDrop) {
  override def drop = SRMultiMap(name, b, keyType.drop, valueType.drop, split, isTop, true)
  override def debugMe(str: String) = logger.debug(
    s"SRMultiMap::$name $str Event=$entry")
  // aux constructor where key is the members(0) and members(1) is the value
  def this(name: String, b: TBranchElement, types: SRComposite, split: Boolean,
    isTop: Boolean) = this(name, b, types.members(0), types.members(1), split, isTop)

  /**
   * explicitly request to read an array of type Map
   * @return Seq[Map] although it will be Seq[Any] typewise
   */
  override def readArray(buffer: RootInput, size: Int) = {
    if (split) {
      debugMe(s"readArray(buffer, $size) in split mode")
      null // assume no splitting for this guy
    }
    else {
      debugMe(s"readArray(buffer, $size) in non-split mode")
      // as for the vector, read the bytecount and version of the map
      val byteCount = buffer.readInt
      val version = buffer.readShort

      if ((version & kMemberWiseStreaming) > 0) {
        null
      }
      else {
        // object wise streaming
        val data = for (i <- 0 until size) yield {
          val nn = buffer.readInt
          // 
          // NOTE: this is a multi map: Map[K, List[V]]
          //
          (for (j <- 0 until nn) yield (keyType.read(buffer), valueType.read(buffer)))
            .groupBy(_._1).mapValues(_.unzip._2)
        }
        entry+=1
        data
      }
    }
  }

  /**
   * explicitly request to read an array of type Map
   * @return Seq[Map] although it will be Seq[Any] typewise
   */
  override def readArray(size: Int) = {
    debugMe(s"readArray($size) calls readArray(buffer, $size)")
    val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeafElement], entry)
    readArray(buffer, size)
  }

  /** 
   * reading by assigning the buffer - we own the branch
   * @return the map of SRType
   */
  override def read = 
    if (split) {
      debugMe(s"read in split mode")
      // current collection has subbranches - this must be an STL node
      // don't check for isTop - a split one must be top
      val leaf = b.getLeaves.get(0).asInstanceOf[TLeaf]
      val buffer = b.setPosition(leaf, entry)

      // for a split collection - size is in the collection leaf node
      val size = buffer.readInt

      // SLT Map never shows up as splittable
      null
    }
    else {
      debugMe(s"read in non-split mode")
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
        // memberwise straeming

        // get the size
        val size = buffer.readInt

        // read
        entry += 1L;
        (for (i <- 0 until size) yield (keyType.read(buffer), 
          valueType.read(buffer))).groupBy(_._1).mapValues(_.unzip._2)
      }
    }

  /**
  * reading by reusing the buffer passed
  */
  override def read(buffer: RootInput) = 
    if (split) {
      debugMe("read(buffer) in split mode")
      // there are subbranches and we are passed a buffer
      // TODO: Do we have such cases???
      null
    }
    else {
      debugMe("read(buffer) in non-split mode")
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
         null
        }
        else {
          // object wise streaming
          val size = buffer.readInt
          entry += 1L;
          (for (i <- 0 until size) yield (keyType.read(buffer), 
            valueType.read(buffer))).groupBy(_._1).mapValues(_.unzip._2)
        }
      }
      else {
        // just read the size and object-wise raeding of all elements
        val size = buffer.readInt
        entry += 1L;
        (for (i <- 0 until size) yield (keyType.read(buffer),
          valueType.read(buffer))).groupBy(_._1).mapValues(_.unzip._2)
      }
    }

  override def hasNext = entry<b.getEntries
  override val toSparkType = MapType(keyType.toSparkType, 
    ArrayType(valueType.toSparkType))
}

/**
 * STL Vector Representation
 * TODO: Do we have MemberWise Streaming for a map???
 */
case class SRMap(
    override val name: String, // actual name if branch is null
    b: TBranchElement, // branch to read from... 
    keyType: SRType, // key type
    valueType: SRType, // value type
    split: Boolean, // does it have subbranches
    isTop: Boolean, // is this map nested in another collection?
    override val _shouldDrop: Boolean = false
    ) extends SRCollection(name, isTop, _shouldDrop) {
  override def drop = SRMap(name, b, keyType.drop, valueType.drop,
    split, isTop, true)
  override def debugMe(str: String) = logger.debug(s"SRMap::$name $str Event=$entry")
  // aux constructor where key is the members(0) and members(1) is the value
  def this(name: String, b: TBranchElement, types: SRComposite, split: Boolean,
    isTop: Boolean) = this(name, b, types.members(0), types.members(1), split, isTop)

  /**
   * explicitly request to read an array of type Map
   * @return Seq[Map] although it will be Seq[Any] typewise
   */
  override def readArray(buffer: RootInput, size: Int) = {
    if (split) {
      debugMe(s"readArray(buffer, $size) in split mode")
      null // assume no splitting for this guy
    }
    else {
      debugMe(s"readArray(buffer, $size) in non-split mode")
      // as for the vector, read the bytecount and version of the map
      val byteCount = buffer.readInt
      val version = buffer.readShort

      if ((version & kMemberWiseStreaming) > 0) {
        null
      }
      else {
        // object wise streaming
        val data = for (i <- 0 until size) yield {
          val nn = buffer.readInt
          (for (j <- 0 until nn) yield (keyType.read(buffer), valueType.read(buffer))).toMap
        }
        entry+=1
        data
      }
    }
  }

  /**
   * explicitly request to read an array of type Map
   * @return Seq[Map] although it will be Seq[Any] typewise
   */
  override def readArray(size: Int) = {
    debugMe(s"readArray($size) calls readArray(buffer, $size)")
    val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeafElement], entry)
    readArray(buffer, size)
  }

  /** 
   * reading by assigning the buffer - we own the branch
   * @return the map of SRType
   */
  override def read = 
    if (split) {
      debugMe(s"read in split mode")
      // current collection has subbranches - this must be an STL node
      // don't check for isTop - a split one must be top
      val leaf = b.getLeaves.get(0).asInstanceOf[TLeaf]
      val buffer = b.setPosition(leaf, entry)

      // for a split collection - size is in the collection leaf node
      val size = buffer.readInt

      // SLT Map never shows up as splittable
      null
    }
    else {
      debugMe(s"read in non-split mode")
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
        // memberwise straeming

        // get the size
        val size = buffer.readInt

        // read
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
      debugMe("read(buffer) in split mode")
      // there are subbranches and we are passed a buffer
      // TODO: Do we have such cases???
      null
    }
    else {
      debugMe("read(buffer) in non-split mode")
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
         null
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
    isTop: Boolean, // is this vector nested in another collection?
    override val _shouldDrop: Boolean = false
    ) extends SRCollection(name, isTop, _shouldDrop) {
  override def drop = SRVector(name, b, t.drop, split, isTop, true)
  override def debugMe(str: String) = logger.debug(s"SRVector::$name $str Event=$entry")

  /**
   * explicitly request to read an array of type Vector
   * @return Seq[Vector] although it will be Seq[Any] typewise
   */
  override def readArray(buffer: RootInput, size: Int) = {
    //
    // NOTE:
    // This can happen when this collection is nested inside some 
    // other splitted collection, STL Node.
    //

    if (split) {
      debugMe(s"readArray(buffer, $size) in split mode")
      null // assume no splitting for this guy
    }
    else {
      debugMe(s"readArray(buffer, $size) in non-split mode")
      val byteCount = buffer.readInt
      val version = buffer.readShort
      if ((version & kMemberWiseStreaming) > 0) {
        // memberwise streaming

        val data = 
          if (t.isInstanceOf[SRNull] || t.isInstanceOf[SRUnknown]) {
            for (i <- 0 until size) yield Seq()
          }
          else {
            val memberVersion = buffer.readShort
            if (memberVersion == 0) buffer.readInt

            // cast to composite
            val composite = t.asInstanceOf[SRComposite]

            // have to transpose
            for (i <- 0 until size) yield {
              val nn = buffer.readInt
              if (nn == 0) Seq()
//              else (for (x <- composite.members) yield x.readArray(buffer, nn)
              else (
                for (x <- composite.members; fieldData=x.readArray(buffer, nn) 
                    if !x.shouldDrop) 
                  yield fieldData
              ).transpose.map(Row.fromSeq(_))
            }
          }
        entry += 1L
        data
      }
      else {
        // objectwise streaming
        val data = for (i <- 0 until size) yield {
          val nn = buffer.readInt
          debugMe(s"readArray(buffer, $size) object-wise current $nn")
          for (j <- 0 until nn) yield t.read(buffer)
        }
        entry += 1L
        data
      }
    }
  }

  /**
   * explicitly request to read an array of type Map
   * @return Seq[Map] although it will be Seq[Any] typewise
   */
  override def readArray(size: Int) = {
    //
    // NOTE:
    // this can happen for a collection inside some STL Node.
    // STL Node will be splitted and this nested collection will be occupying its own
    // branch. That branch will not get splitted further!
    // Therefore we simply assign the buffer for this branch and call 
    // readArray(buffer,..).
    //
    debugMe(s"readArray($size) calls readArray(buffer, $size)")
    val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeafElement], entry)
    readArray(buffer, size)
  }

  /** 
   * reading by assigning the buffer - we own the branch
   * @return the vector of SRType
   */
  override def read = 
    if (split) {
      debugMe(s"read in split mode")
      // current collection has subbranches - this must be an STL node
      // don't check for isTop - a split one must be top
      val leaf = b.getLeaves.get(0).asInstanceOf[TLeaf]
      val buffer = b.setPosition(leaf, entry)

      // for a split collection - size is in the collection leaf node
      val size = buffer.readInt

      val data = 
        if (t == t.isIntanceOf[SRNull] || t.isInstanceOf[SRUnknown]) Seq()
        else {
          val composite = t.asInstanceOf[SRComposite]

          // we get Seq(f1[size], f2[size], ..., fN[size])
          // we just have to transpose it
          //
          // For the case of a split collection, the fields of the composite 
          // should've been filtered out during the ATT pruning
          (for (x <- composite.members)
            yield {
              // read array for each field, members will assign the buffer 
              // themselves
              x.readArray(size)
            }).transpose.map(Row.fromSeq(_))
        }
      entry += 1L
      data
    }
    else {
      debugMe(s"read in non-split mode")
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
        //
        // TODO:
        // for MemberWise Streaming we read w/o incrementing the event counters 
        // but have to explicitly increment the counter for them!
        val data = 
          if (t.isInstanceOf[SRNull] || t.isInstanceOf[SRUnknown]) Seq()
          else {
            val composite = t.asInstanceOf[SRComposite]

            // member Version
            val memberVersion = buffer.readShort
            // if 0 - read checksum
            if (memberVersion == 0) buffer.readInt

            // now read the size of the vector
            val size = buffer.readInt

            // have to transpose
            (for (x <- composite.members; fieldData=x.readArray(buffer, size)
              if !x.shouldDrop)
              yield fieldData).transpose.map(Row.fromSeq(_))
          }
        entry += 1L
        data
      }
      else {
        // get the size
        val size = buffer.readInt
        logger.debug(s"read in non-split mode object-wise $size")
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
      debugMe(s"read(buffer) in split mode")
      null
    }
    else {
      debugMe(s"read(buffer) in non-split mode")
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
          val data = if (t.isInstanceOf[SRNull] || t.isInstanceOf[SRUnknown]) Seq()
          else {
            val composite = t.asInstanceOf[SRComposite]

            // memeberVersion
            val memberVersion = buffer.readShort
            // if 0 - read checksum
            if (memberVersion == 0) buffer.readInt

            // size 
            val size = buffer.readInt

            // have to transpose
            (for (x <- composite.members; fieldData=x.readArray(buffer, size)
              if !x.shouldDrop)
              yield fieldData
              //we own the buffer
              // increment the entry
            ).transpose.map(Row.fromSeq(_))
          }
          entry += 1L
          data
        }
        else {
          // object wise streaming
          val size = buffer.readInt
          debugMe(s"read(buffer) in object-wise mode $size as TOP")
          entry += 1L;
          for (i <- 0 until size) yield t.read(buffer)
        }
      }
      else {
        // just read the size and object-wise raeding of all elements
        val size = buffer.readInt
        debugMe(s"read(buffer) in object-wise mode $size as _not_ TOP")
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
    split: Boolean, // is it split - are there sub branches
    isTop: Boolean, // top branch composite doens't read the header
    isBase: Boolean = false, // is this composite a base class or not
    override val _shouldDrop: Boolean = false
    ) extends SRType(name, _shouldDrop) {

  override def drop = SRComposite(name, b, members.map(_.drop), split, isTop,
    isBase, true)

  override def debugMe(str: String) = logger.debug(s"SRComposite::$name $str Event=$entry")
  override def readArray(size: Int) = {
    // 
    // This will happen when we have a composite inside 
    // a splittable collection of objects - STL Node.
    // This can come in 2 forms -
    //

    if (split) {
      debugMe(s"readArray($size) in split mode")
      // if this is split - simply pass to members
      // basically this composite is HOLLOW!
      val data = 
        if (members.size==0 ) 
          for (i <- 0 until size) yield Row()
        else
          // when the Composite is split -> fields must've been filtered out 
          // at hte point of ATT Construction
          (for (m <- members) yield m.readArray(size)).
            transpose.map(Row.fromSeq(_))
      entry += 1L
      data
    }
    else {
      debugMe(s"readArray($size) in non-split mode calls readArray(buffer, $size)")
      // this is not splittable - so we must assign the buffer and read
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeafElement], entry)
      readArray(buffer, size)
    }
  }

  // this is for memberwise reading
  override def readArray(buffer: RootInput, size: Int) = {
    if (isBase) {
      debugMe(s"readArray(buffer, $size) in isBase mode")
      // this guy is hollow  we just pass -> typically for BASE classes
      val data = 
        if (members.size == 0)
          for (i <- 0 until size) yield Row()
        else
          (for (m <- members; fieldData=m.readArray(buffer, size) 
            if !m.shouldDrop) yield fieldData).transpose.map(Row.fromSeq(_))
      entry += 1L
      data
    }
    else {
      debugMe(s"readArray(buffer, $size) in non-base mode")
      // this guy is not hollow - typically for composites that are not BASE
      // when there is no splitting
      val data = for (i <- 0 until size) yield {
        /*
         * TODO: Do we need to read byte count in here?! as of 20.01.2017
         */
        //val byteCount = buffer.readInt
        //val byteCount = buffer.readInt
        val version = buffer.readVersion
        if (version == 0) buffer.readInt

        Row.fromSeq(for (m <- members; fieldData=m.read(buffer)
          if !m.shouldDrop) yield fieldData)
      }
      entry += 1L
      data
    }
  }

  /**
   * reading by assigning the buffer.
   * 1. For a class that is split =>
   * - means that all the members are separate and do not need the buffer.
   * 2. For a class that is not split =>
   * - means that members will be contiguously stored and require the buffer to be passed
   *   downstream
   * - m
   */
  override def read: Any = 
    if (split) {
      debugMe(s"read in split mode")
      // split class -- just pass the call to members
      // do not have to read the header information
      entry+=1L
      // Split mode -> not needed fields must've been filtered out 
      // at the point of the ATT construction.
      Row.fromSeq(for (m <- members) yield m.read)
    }
    else {
      debugMe(s"read in non-split mode")
      // composite is not split into subbranches for members
      // get the buffer
      // if this composite is empty and has its own branch, nothing to read.
      // version and other information 
      if (members.length == 0) { entry+=1L; return Row()}
      val buffer = b.setPosition(b.getLeaves.get(0).asInstanceOf[TLeaf], entry)

      // check if this branch is top level or not
      if (isTop) {
        // top level type-branch
        // do not read the header information
        entry+=1L
        // pass the buffer downstream
        Row.fromSeq(for (m <- members; fieldData=m.read(buffer)
          if !m.shouldDrop) yield fieldData)
      }
      else {
        /*
         * TODO: Is this correct???
         */
        // not a top level type-branch
        // we have to read the header
        //val byteCount = buffer.readInt
        val version = buffer.readVersion
        // if 0 - read checksum
        if (version==0) buffer.readInt
        
        entry += 1L
        Row.fromSeq(for (m <- members; fieldData=m.read(buffer)
          if !m.shouldDrop) yield fieldData)
      }
    }

  /**
   * reading by reusing the buffer
   */
  override def read(buffer: RootInput) = {
    debugMe(s"read(buffer)")
    // not a top branch
    // can not be split - composite that receive the buffer are contiguous

    entry+=1L
    
    /*
     * TODO: Is this corect ???
     */
    // read the header
    //val byteCount = buffer.readInt
    val version = buffer.readVersion
    if (version == 0) buffer.readInt

    // read
    Row.fromSeq(for (m <- members; fieldData=m.read(buffer)
      if !m.shouldDrop) yield fieldData)
  }

  override def hasNext = entry<b.getEntries
  override val toSparkType = StructType(
    for (t <- members if !t.shouldDrop) yield StructField(t.toName, t.toSparkType)
  )
}

// companion
object SRComposite {
  def apply(other: SRComposite): SRComposite = SRComposite(other.name,
    other.b, other.members, other.split, other.isTop, other.isBase, other._shouldDrop)
}
