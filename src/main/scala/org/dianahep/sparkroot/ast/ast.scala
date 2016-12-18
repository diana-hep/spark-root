package org.dianahep.sparkroot

import org.dianahep.sparkroot.ast._
import org.dianahep.root4j.interfaces._
import org.dianahep.root4j.core._
import org.dianahep.root4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

package object ast
{

  /*
   * LeafInfo - simple TLeaf info
   * LeafElement - TLeafElement info
   */
  class LeafInfo(val name: String, val className: String, val nElements: Int)
  {
    override def toString = (name, className, nElements).toString
  }
  class LeafElementInfo(override val name: String, override val className: String,
    override val nElements: Int, val myTypeCode: Int) 
    extends LeafInfo(name, className,nElements)
  {
    override def toString = (name, className, nElements, myTypeCode).toString
  }

  /*
   * NodeInfo - simple Tbranch info
   * NodeElementInfo - TBranchElement info
   */
  class NodeInfo(val name: String, val title: String, val className: String,
    val myType: SRType)
  {
    override def toString = (name, title, className, myType).toString
  }
  class NodeElementInfo(override val name: String, override val title: String, 
    override val className: String, 
    override val myType: SRType, val parentName: String,
    val streamerTypeCode: Int, val myTypeCode: Int, val objClassName: String,
    val id: Int) 
    extends NodeInfo(name, title, className, myType)
  {
    override def toString = (name, title, className, myType, parentName, 
      streamerTypeCode,  myTypeCode, objClassName, id).toString
  }

  abstract class AbstractSchemaTree;
  case class RootNode(name: String, nodes: Seq[AbstractSchemaTree]) 
    extends AbstractSchemaTree;
  case class EmptyRootNode(val name: String, var entries: Long) extends AbstractSchemaTree;

  //  simple TBranch/TLeaf representations
  class Leaf(val info: LeafInfo) extends AbstractSchemaTree;
  case class TerminalNode(leaf: Leaf, info: NodeInfo, 
    iter: BasicBranchIterator) extends AbstractSchemaTree;
  case class TerminalMultiLeafNode(leaves: Seq[Leaf], info: NodeInfo,
    iter: StructBranchIterator) extends AbstractSchemaTree;
  case class Node(subnodes: Seq[AbstractSchemaTree], info: NodeInfo) 
    extends AbstractSchemaTree;

  //  TBranchElement/TLeafElement representations
  class LeafElement(override val info: LeafElementInfo) extends Leaf(info);

  // represents a splittable collection branch
  case class SplittableCollectionNodeElement(subnodes: Seq[AbstractSchemaTree], 
    leaf: LeafElement,
    info: NodeElementInfo) 
    extends AbstractSchemaTree;

  // represents a splittable object sitting in a branch
  case class SplittableObjectNodeElement(subnodes: Seq[AbstractSchemaTree],
    info: NodeElementInfo)
    extends AbstractSchemaTree;

  // represents a unsplittable collection branch: either nestedness>1 or 
  // a collection of simple types
  case class UnSplittableCollectionNodeElement(leaf: LeafElement,
    info: NodeElementInfo,
    streamer: TStreamerElement,
    iter: BranchIterator[Any])
    extends AbstractSchemaTree;

  // unsplittable object sitting in a branch - not a collection
  case class UnSplittableObjectNodeElement(leaf: LeafElement,
    info: NodeElementInfo,
    streamers: Seq[TStreamerElement], 
    iter: BranchIterator[Any]) extends AbstractSchemaTree;

  // a terminal node of the tree - not in collection of nestedness 1
  case class TerminalSimpleNodeElement(leaf: LeafElement,
    info: NodeElementInfo,
    iter: BranchIterator[Any]) extends AbstractSchemaTree;

  // a terminal node of the tree - identical to unsplittableObject
  // TODO: Do we need this guy???
  case class TerminalObjectNodeElement(leaf: LeafElement,
    info: NodeElementInfo, iter:BranchIterator[Any])
    extends AbstractSchemaTree;

  // Terminal Collection Member - for nestedness level of 1, members are split
  case class TerminalCollectionMemberNodeElement(leaf: LeafElement, info:NodeElementInfo,
    iter: BranchIterator[Any])
    extends AbstractSchemaTree;

  // Unknown guys
  case class UnknownNode(subnodes: Seq[AbstractSchemaTree], leaf: LeafElement,
    info: NodeElementInfo)
    extends AbstractSchemaTree;
  case class UnknownTerminalNode(leaf: Leaf, info: NodeElementInfo, iter: BranchIterator[Any]) 
    extends AbstractSchemaTree;

  /**
   * for simple branches - these are the ROOT Type/Codes => our internal type system
   * @return - return the DataType representing the code
   */
  def assignLeafTypeByLeafClass(leaf: TLeaf): SRType = 
    leaf.getRootClass.getClassName.last match
  {
    case 'C' => SRStringType
    case 'B' => SRByteType
    case 'b' => SRByteType
    case 'S' => SRShortType
    case 's' => SRShortType
    case 'I' => SRIntegerType
    case 'i' => SRIntegerType
    case 'F' => SRFloatType
    case 'D' => SRDoubleType
    case 'L' => SRLongType
    case 'l' => SRLongType
    case 'O' => SRBooleanType
    case _ => SRNull
  }

  /**
   * @return - Return the full Simple SR Data Type for this terminal branch
   */
  def assignBranchType(branch: TBranch): SRType = 
  {
    val leaves = branch.getLeaves
    if (leaves.size > 1) SRStructType(
      for (i <- 0 until leaves.size; leaf=leaves.get(i).asInstanceOf[TLeaf])
        yield (leaf.getName, assignLeafType(leaf))
    )
    else assignLeafType(leaves.get(0).asInstanceOf[TLeaf])
  }

  def assignLeafType(leaf: TLeaf): SRType = 
  {
    if (leaf.getArrayDim>0) // array
      SRArrayType(assignLeafTypeByLeafClass(leaf), leaf.getArrayDim)
    else
      assignLeafTypeByLeafClass(leaf)
  }

  /**
   * @return prints the Abstractly Typed Tree
   */
  def printATT(att: core.SRType, level: Int = 0, sep: String = "  "): Unit = att match {
    case core.SRNull => println(sep*level+"Null")
    case core.SRRoot(name, entries, types) => {
      println(s"Root: $name wtih $entries Entries")
      for (t <- types) printATT(t, level+1)
    }
    case core.SREmptyRoot(name, entries) =>
      println(s"Empty Root: $name with $entries Entries")
    case core.SRInt(name, _, _) => println(sep*level+s"$name: Integer")
    case core.SRString(name, _, _) => println(sep*level+s"$name: String")
    case core.SRLong(name, _, _) => println(sep*level+s"$name: Long")
    case core.SRDouble(name, _, _) => println(sep*level+s"$name: Double")
    case core.SRByte(name, _, _) => println(sep*level+s"$name: Byte")
    case core.SRBoolean(name, _, _) => println(sep*level+s"$name: Boolean")
    case core.SRFloat(name, _, _) => println(sep*level+s"$name: Float")
    case core.SRArray(name, _, _, t, n) => {
      println(sep*level + s"$name: Array[$n]")
      printATT(t, level+1)
    }
    case core.SRVector(name, _, t, split, isTop) => {
      println(sep*level + s"$name: STL Vector. split=$split and isTop=$isTop")
      printATT(t, level+1)
    }
    case core.SRMap(name, _, keyType, valueType, split, isTop) => {
      println(sep*level + s"$name: Map ${keyType.name} => ${valueType.name}. split=$split and isTop=$isTop")
      println(sep*(level+1) + "Key Type:")
      printATT(keyType, level+2)
      println(sep*(level+1) + "Value Type:")
      printATT(valueType, level+2)
    }
    case core.SRSTLString(name, _, isTop) => {
      println(sep*level + s"$name: STL String isTop=$isTop")
    }
    case core.SRComposite(name, b, members, split, isTop, isBase) => {
      println(sep*level + s"${name}: Composite split=$split isTop=$isTop isBase=$isBase")
      for (t <- members) printATT(t, level+1)
    }
    case _ => println("")
  }

  def buildSparkSchema(att: core.SRType) = att.toSparkType.asInstanceOf[StructType]
  def readSparkRow(att: core.SRType): Row = att.read.asInstanceOf[Row]
  def containsNext(att: core.SRType) = att.hasNext

  /**
   * Build ATT 
   *
   * @return ATT
   */
  def buildATT(
    tree: TTree,
    streamers: Map[String, TStreamerInfo],
    requiredColumns: Array[String],
    debug: Int = 0 // 0 for no debug messages printed
  ): core.SRType = {

    def synthesizeLeafType(b: TBranch, leaf: TLeaf): core.SRType = 
      leaf.getRootClass.getClassName.last match {
        case 'C' => core.SRString(leaf.getName, b, leaf)
        case 'B' => core.SRByte(leaf.getName, b, leaf)
        case 'b' => core.SRByte(leaf.getName, b, leaf)
        case 'S' => core.SRShort(leaf.getName, b, leaf)
        case 's' => core.SRShort(leaf.getName, b, leaf)
        case 'I' => core.SRInt(leaf.getName, b, leaf)
        case 'i' => core.SRInt(leaf.getName, b, leaf)
        case 'F' => core.SRFloat(leaf.getName, b, leaf)
        case 'D' => core.SRDouble(leaf.getName, b, leaf)
        case 'L' => core.SRLong(leaf.getName, b, leaf)
        case 'l' => core.SRLong(leaf.getName, b, leaf)
        case 'O' => core.SRBoolean(leaf.getName, b, leaf)
        case _ => core.SRNull
    }

    def synthesizeLeaf(b: TBranch, leaf: TLeaf): core.SRType = {
      def iterate(dimsToGo: Int): core.SRType =
        if (dimsToGo==1) core.SRArray(leaf.getName, b, leaf, synthesizeLeafType(b, leaf), 
          leaf.getMaxIndex()(leaf.getArrayDim-1))
        else
          core.SRArray(leaf.getName, b, leaf, iterate(dimsToGo-1), leaf.getMaxIndex()(
            leaf.getArrayDim-dimsToGo))

      if (leaf.isInstanceOf[TLeafElement])
        // leafElement
        synthesizeLeafElement(b, leaf.asInstanceOf[TLeafElement])
      else {
        // leaf
        if (leaf.getArrayDim==0)
          synthesizeLeafType(b, leaf)
        else
          iterate(leaf.getArrayDim)
      }
    }

    def synthesizeLeafElement(b: TBranch, leaf: TLeafElement): core.SRType = {
      return core.SRNull;
    }

    /**
     * top branch is special
     */
    def synthesizeTopBranch(b: TBranch): core.SRType = {
      if (b.isInstanceOf[TBranchElement]) {
        val be = b.asInstanceOf[TBranchElement]
        val streamerInfo = streamers.applyOrElse(be.getClassName,
          (x: String) => null)
        if (streamerInfo==null) 
          // a splitted vector doesn't show up in the TStreamerInfo
          // start to analyze the class name
          synthesizeClassName(be.getClassName, be, core.SRRootType)
        else
          // majority should be present
          synthesizeStreamerInfo(be, streamerInfo, null, core.SRRootType)
      }
      else { // simple TBranch case
        val leaves = b.getLeaves
        if (leaves.size>1) // multi leaf branch
          new core.SRComposite(b.getName, b, 
            for (i <- 0 until leaves.size) yield synthesizeLeaf(b, 
              leaves.get(i).asInstanceOf[TLeaf]), true, true)
        else  // a single leaf branch
          synthesizeLeaf(b, leaves.get(0).asInstanceOf[TLeaf])
      }
    }
    
    /*
     * for the case when we have a basic type nested - 
     * it doesn't need to have a name or branch...
     */
    def synthesizeBasicStreamerType(typeCode: Int): core.SRType = typeCode match {
      case 1 => core.SRByte("", null, null)
      case 2 => core.SRShort("", null, null)
      case 3 => core.SRInt("", null, null)
      case 4 => core.SRLong("", null, null)
      case 5 => core.SRFloat("", null, null)
      case 6 => core.SRInt("", null, null)
      case 7 => core.SRString("", null, null)
      case 8 => core.SRDouble("", null, null)
      case 9 => core.SRFloat("", null, null)
      case 10 => core.SRByte("", null, null)
      case 11 => core.SRByte("", null, null)
      case 12 => core.SRShort("", null, null)
      case 13 => core.SRInt("", null, null)
      case 14 => core.SRLong("", null, null)
      case 15 => core.SRInt("", null, null)
      case 16 => core.SRLong("", null, null)
      case 17 => core.SRLong("", null, null)
      case 18 => core.SRBoolean("", null, null)
      case 19 => core.SRShort("", null, null)
      case _ => core.SRNull
    }

    def synthesizeStreamerElement(
      b: TBranchElement, 
      streamerElement: TStreamerElement,
      parentType: core.SRTypeTag
      ): core.SRType = {

      // when you have an array of something simple kOffsetL by ROOT convention  
      def iterateArray(dimsToGo: Int): core.SRType = 
        if (dimsToGo==1) core.SRArray(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement], 
          synthesizeBasicStreamerType(streamerElement.getType-20),
          streamerElement.getMaxIndex()(streamerElement.getArrayDim-1))
      else
        core.SRArray(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement], 
          iterateArray(dimsToGo-1), streamerElement.getMaxIndex()(
            streamerElement.getArrayDim-dimsToGo))

      streamerElement.getType match {
        case 0 => { // BASE CLASS
          // assume for now that the inheritance is from composite classes
          // NOTE: get the name instead of type name for the super class
          val streamerInfo = streamers.applyOrElse(streamerElement.getName,
          (x: String) => null)
          if (streamerInfo==null) core.SRNull
          else {
            // in principle there must be the TStreamerInfo for all the bases 
            // used
            if (debug>0) println(s"There is a class name for: ${streamerElement.getName}")
            if (streamerInfo.getElements.size==0) 
              // empty BASE CLASS, create an empty composite
              // splitting does not matter - it's empty
              core.SRComposite(streamerElement.getName, b, Seq(), false, false)
            else
              synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
          }
        }
        case 1 => core.SRByte(streamerElement.getName, b, 
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 2 => core.SRShort(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 3 => core.SRInt(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 4 => core.SRLong(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 5 => core.SRFloat(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 6 => core.SRInt(streamerElement.getName, b, 
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 7 => core.SRString(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 8 => core.SRDouble(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 9 => core.SRFloat(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 10 => core.SRByte(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 11 => core.SRByte(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 12 => core.SRShort(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 13 => core.SRInt(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 14 => core.SRLong(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 15 => core.SRInt(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 16 => core.SRLong(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 17 => core.SRLong(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 18 => core.SRBoolean(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 19 => core.SRShort(streamerElement.getName, b, 
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case it if 21 until 40 contains it => iterateArray(streamerElement.getArrayDim)
        case 61 => {
          // NOTE: get the type name
          val streamerInfo = streamers.applyOrElse(streamerElement.getTypeName,
          (x: String) => null)
          if (streamerInfo==null) core.SRNull
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }
        case 62 => {
          // NOTE: get the typename
          val streamerInfo = streamers.applyOrElse(streamerElement.getTypeName,
            (x: String) => null)
          if (streamerInfo==null) core.SRNull
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }
        case 500 => synthesizeStreamerSTL(b, streamerElement.asInstanceOf[TStreamerSTL],
          parentType)
        case _ => core.SRNull
      }
    }

    /**
     * @return the full type definition for a basic type
     *
     * Array is also a basic type - leave it out for now
     */
    def synthesizeBasicTypeName(
      typeName: String // basic type name
    ): core.SRType = {

      typeName match {
        case "int" => core.SRInt("", null, null)
        case "float" => core.SRFloat("", null, null)
        case "double" => core.SRDouble("", null, null)
        case "char" => core.SRByte("", null, null)
        case "long" => core.SRLong("", null, null)
        case "short" => core.SRShort("", null, null)
        case "bool" => core.SRBoolean("", null, null)
        case "unsigned int" => core.SRInt("", null, null)
        case "unsigned char" => core.SRByte("", null, null)
        case "unsigned long" => core.SRLong("", null, null)
        case "unsigned short" => core.SRShort("", null, null)

        // ROOT ones ending with t
        case "Double32_t" => core.SRFloat("", null, null)
        case _ => core.SRNull
      }
    }

    /**
     * @return the full type definition from known types
     *
     * assumptions: no TStreamerInfo for the class itself
     * 1. Either this is contained within some STL and should be STL or pair itself
     * 2. This is STL for top branch or std::pair
     *
     * assume for now that this is one of the following:
     * 1) STL Collection
     * 2) std::array ???
     * 3) pair<T, U>
     *
     * we basically assume that if something is not present, it must be something of STL
     */
    def synthesizeClassName(
      className: String, // c++ standard class type declaration (w/ spaces for templ.)
      b: TBranchElement, // if the branch is split, we still need it
      parentType: core.SRTypeTag // the tag for what our parent is
    ): core.SRType = {
      val stlLinear = Seq("vector", "list", "deque", "set", "multiset",
        "forward_list", "unordered_set", "unordered_multiset")
      val stlAssociative = Seq("map", "unordered_map", "multimap", "unordered_multimap")
      val stlPair = "pair"
      val stlStrings = Seq("string", "__basic_string_common<true>")
      
      // quickly parse the class type and template argumetns
      val classTypeRE = "(.*?)<(.*?)>".r
      if (debug>0) println(s"classType being synthesized: ${className} ${className.trim.length} ${className.length}")
      val (classTypeString, argumentsTypeString) = className match {
        case classTypeRE(aaa,bbb) => (aaa,bbb.trim)
        case _ => (null, null)
      }

      if (debug>0) println(s"Parsed classType=$classTypeString and argumentsType=$argumentsTypeString")

      // check if this is a string
      // and return if it is
      if (stlStrings contains className) {
        return if (b == null) 
          parentType match {
            case core.SRCollectionType => core.SRSTLString("", null, false)
            case _ => core.SRSTLString("", null, true)
          }
        else 
          parentType match {
            case core.SRCollectionType => core.SRSTLString(b.getName, b, false)
            case _ => core.SRSTLString(b.getName, b, true)
          }
      }

      // if parsing is unsuccessful, assign null
      if (classTypeString == null || argumentsTypeString == null)
        return core.SRNull

      // based on the class type name
      classTypeString match {
        case it if stlLinear contains it => {
          // we have something that is vector like
          // arguments must be a single typename
          // 1. check if it's a basic type name
          val streamerInfo = streamers.applyOrElse(argumentsTypeString,
            (x: String) => null)
          val valueType = 
            if (streamerInfo == null) {
              // no streamer info for value type
              // is it a basic type
              // else synthesize the name again
              val basicType = synthesizeBasicTypeName(argumentsTypeString)
              if (basicType == core.SRNull)
                // not a basic type
                // can not be composite class - must have a TStreamerInfo
                // should be some STL - nested => no subbranching
                synthesizeClassName(argumentsTypeString, null,
                  core.SRCollectionType) 
              else basicType
            }
            else 
              // there is a TStreamerInfo
              // NOTE: this applies to a top STL node as well for which b != null
              synthesizeStreamerInfo(b, streamerInfo, null, core.SRCollectionType)

          // TODO: we need to do each collection separately???
          // that is only the case when we have version for each STL separately read in
          if (b==null) 
            parentType match {
              // this is not the top collection
              case core.SRCollectionType => core.SRVector("", b, valueType,false, false)
              // this is the top collection
              case _ => core.SRVector("", b, valueType, false, true)
            }
          else
            parentType match {
              // branch is not null for the vector
              // parent is collection - could not happen in principle...
              case core.SRCollectionType => core.SRVector(b.getName, b, valueType, false, false)
              // top STL node will be here...
              case _ => core.SRVector(b.getName, b, valueType,
                if (b.getBranches.size==0) false else true, true)
            }
        }
        case it if stlAssociative contains it => {
          // we have something that is map like
          // extract the key/value type names
          println(s"Synthesizing the current class arguments: ${classTypeString}")
          val mapTemplateRE = "(.*?),(.*?)".r
          val (keyTypeString, valueTypeString) = argumentsTypeString match {
            case mapTemplateRE(aaa,bbb) => (aaa,bbb)
            case _ => (null, null)
          }

          // if there is a matching issue - assign null
          if (keyTypeString==null || valueTypeString==null) return core.SRNull

          // key type is a basic type - assumed for now.
          val keyType = 
            if (synthesizeBasicTypeName(keyTypeString) == core.SRNull)
              // no our key is not a basic type.... for now put null
              core.SRNull
            else
              synthesizeBasicTypeName(keyTypeString)

          // value type
          val streamerInfo = streamers.applyOrElse(valueTypeString,
            (x: String) => null)
          val valueType =
            if (streamerInfo == null) {
              // no streamer info
              // is basic type
              // else synthesize the name again
              val basicType = synthesizeBasicTypeName(valueTypeString)
              if (basicType == core.SRNull)
                // not a basic type
                synthesizeClassName(valueTypeString, null,
                  core.SRCollectionType) 
              else basicType
            }
            else {
              // there is a TStreamerInfo
              synthesizeStreamerInfo(b, streamerInfo, null,
                core.SRCollectionType)
            }

          // TODO: we need to do each collection separately???
          // that is only the case when we have version for each STL separately read in
          if (b==null) 
            parentType match {
              // this is not the top collection
              case core.SRCollectionType => new core.SRMap("", b, keyType,
                valueType, false, false)
              // this is the top collection
              case _ => new core.SRMap("", b, keyType, valueType, false, true)
            }
          else
            parentType match {
              case core.SRCollectionType => new core.SRMap(b.getName, b, keyType,
                valueType, false, false)
              // if there are multiple sub branches of this guy - it's split
              case _ => new core.SRMap(b.getName, b, keyType, valueType,
                if (b.getBranches.size==0) false else true, true)
            }
        }
        case it if it == stlPair => {
          // pair is considered to be the composite
          val pairTemplateRE = "(.*?),(.*?)".r
          val (firstTypeString, secondTypeString) = argumentsTypeString match {
            case pairTemplateRE(aaa,bbb) => (aaa,bbb)
            case _ => (null, null)
          }

          if (debug>0) println(s"We got a pair: first=$firstTypeString second=$secondTypeString")

          // if there is a matching issue - assign null
          if (firstTypeString==null || secondTypeString==null) return core.SRNull

          //  streamer info for first/second
          val streamerInfoFirst = streamers.applyOrElse(firstTypeString,
            (x: String) => null)
          val streamerInfoSecond = streamers.applyOrElse(secondTypeString,
            (x: String) => null)

          // get the type for first
          val firstType =
            if (streamerInfoFirst == null) {
              // no streamer info
              // is basic type
              // else synthesize the name again
              val basicType = synthesizeBasicTypeName(firstTypeString)
              if (basicType == core.SRNull)
                // not a basic type
                synthesizeClassName(firstTypeString, 
                  if (b==null) null
                  else if (b.getBranches.size==0) null
                  else b.getBranches.get(0).asInstanceOf[TBranchElement],
                  core.SRCompositeType) 
              else basicType
            }
            else {
              // there is a TStreamerInfo
              synthesizeStreamerInfo(null, streamerInfoFirst, null,
                core.SRCompositeType)
            }

          // get the type for second
          val secondType =
            if (streamerInfoSecond == null) {
              // no streamer info
              // is basic type
              // else synthesize the name again
              val basicType = synthesizeBasicTypeName(secondTypeString)
              if (basicType == core.SRNull)
                // not a basic type
                synthesizeClassName(secondTypeString,
                  if (b==null) null
                  else if (b.getBranches.size==0) null
                  else b.getBranches.get(1).asInstanceOf[TBranchElement],
                  core.SRCompositeType) 
              else basicType
            }
            else {
              // there is a TStreamerInfo
              synthesizeStreamerInfo(null, streamerInfoSecond, null,
                core.SRCompositeType)
            }

          // TODO: Do we need a special type for pair???
          // TODO: Can pair be split???
          if (b==null) 
            parentType match {
              // branch is null for this type
              // parent is collection
              case core.SRCollectionType => core.SRComposite("",
                b, Seq(firstType, secondType), false, false)
              // parent is not a collection
              case _ => core.SRComposite("", b, Seq(firstType, secondType), false, false)
            }
          else
            parentType match {
              // parent is collection
              // there is a branch for Collection<pair>
              case core.SRCollectionType => core.SRComposite(b.getName, b,
                Seq(firstType, secondType), 
                if (b.getBranches.size==0) false else true, false)
              case core.SRRootType => core.SRComposite(b.getName, b,
                Seq(firstType, secondType),
                if (b.getBranches.size==0) false else true, true)
              case _ => core.SRComposite(b.getName, b, Seq(firstType, secondType),
                if (b.getBranches.size==0) false else true, false)
            }
        }
        case _ => core.SRNull
      }
    }

    /**
     * @return the SparkROOT type for the STL collection
     */
    def synthesizeStreamerSTL(
      b: TBranchElement,
      streamerSTL: TStreamerSTL,
      parentType: core.SRTypeTag
    ): core.SRType = {
      // debugging...
      if (debug>0) println(s"TStreamer STL for type: ${streamerSTL.getTypeName}")

      // parse by the stl type
      streamerSTL.getSTLtype match {
        case 1 => { // std::vector

          // probe the member type
          val ctype = streamerSTL.getCtype
          val t = 
            if (ctype<61) {
              if (streamerSTL.getTypeName.contains("bool") && 
                ctype == 21)
                // BOOl shows up as ctype 21 - TODO: this needs to be fixed!
                // synthesize this as 18
                synthesizeBasicStreamerType(18)
              else 
                // this is some basic type - synthesize
                synthesizeBasicStreamerType(ctype)
            }
            else {
              val memberClassName = streamerSTL.getTypeName.slice(
                streamerSTL.getTypeName.
                indexOf('<')+1, streamerSTL.getTypeName.length-1).trim
              val streamerInfo = streamers.applyOrElse(memberClassName, 
                (x: String) => null)


              if (streamerInfo==null) 
                // no streamer info - type may be(must be) an STL collection
                // right away - for nested STL no splitting!
                synthesizeClassName(memberClassName, null, core.SRCollectionType)
              else synthesizeStreamerInfo(
                if (b==null) null
                else if (b.getBranches.size==0) null
                else b
                , streamerInfo, 
                null, core.SRCollectionType)
            }

          if (b==null) 
            // nested vector or a vector does not have a separate branch
            parentType match {
              // this is not the top collection
              case core.SRCollectionType => core.SRVector("", b, t ,false, false)
              // this is the top collection
              case _ => core.SRVector(streamerSTL.getName, b, t, false, true)
            }
          else
            parentType match {
              case core.SRCollectionType => core.SRVector(b.getName, b, t, false, false)
              // if there are multiple sub branches of this guy - it's split
              case _ => core.SRVector(b.getName, b, t,
                if (b.getBranches.size==0) false else true, true)
            }
        }
        case 4 => { // std::map
          synthesizeClassName(streamerSTL.getTypeName,
            b, parentType)
          /*
          // probe the key/value types
          val keyValueTypeNames = streamerSTL.getTypeName.slice(
            streamerSTL.getTypeName.indexOf('<')+1, streamerSTL.getTypeName.length-1)

          if (debug>0) println(s"KeyValueTypeNames: ${keyValueTypeNames}")
          // check if the pair of <key_type, value_type> is present in TStreamerInfo
          val streamerInfo = streamers.applyOrElse(s"pair<${keyValueTypeNames}>",
            (x: String) => null)
          if (streamerInfo != null) {
            // there is a streamer info for the pair
            // extract it and use that to identify the member type
            val keyValueTypes = synthesizeStreamerInfo(
              if (b==null) null
              else if (b.getBranches.size==0) null
              else b,
              streamerInfo, null, core.SRCollectionType).asInstanceOf[core.SRComposite]

            // create a map
            if (b==null)
              // nested vector or a vector does not have a separate branch
              parentType match {
                // this is not the Top Level Collection
                case core.SRCollectionType => new core.SRMap("",b,keyValueTypes,
                  false, false)
                // this is the top level collection
                case _ => new core.SRMap(streamerSTL.getName, b, keyValueTypes, false, true)
              }
            else 
              parentType match {
                case core.SRCollectionType => new core.SRMap(b.getName, b, keyValueTypes,
                  false, false)
                case _ => new core.SRMap(b.getName, b, keyValueTypes, 
                  if (b.getBranches.size==0) false else true, true)
              }
          }
          else {
            // pair<keytype, valuetype> is not in the TStreamerInfo
            // we build the pair ourselves and send it to be synthesized by
            // class name... TODO: Is extracting the key/value types explicitly better?
            //
            // TODO: This assumes that there is no splitting of the branch
            // synthesis of pair's class name doesn't accomodate splitting of branch
            // synthesis of pair's TStreamerInfo will - since it's a composite
            val keyValueTypes = synthesizeClassName(
              // note the keyValueTypeNames has not been trimmed - c++ standard type
              s"pair<${keyValueTypeNames}>", b, core.SRCollectionType
            ).asInstanceOf[core.SRComposite]
            // create a map
            if (b==null)
              // nested vector or a vector does not have a separate branch
              parentType match {
                // this is not the Top Level Collection
                case core.SRCollectionType => new core.SRMap("",b,keyValueTypes,
                  false, false)
                // this is the top level collection
                case _ => new core.SRMap(streamerSTL.getName, b, keyValueTypes, false, true)
              }
            else parentType match {
              case core.SRCollectionType => new core.SRMap(b.getName, b, keyValueTypes,
                false, false)
              case _ => new core.SRMap(b.getName, b, keyValueTypes, 
                if (b.getBranches.size==0) false else true, true)
            }
          }
          */
        }
        case 365 => { // std::string
          if (b == null) 
            parentType match {
              case core.SRCollectionType => core.SRSTLString("", null, false)
              case _ => core.SRSTLString(streamerSTL.getName, null, true)
            }
          else 
            parentType match {
              case core.SRCollectionType => core.SRSTLString(b.getName, b, false)
              case _ => core.SRSTLString(streamerSTL.getName, b, true)
            }
        }
        case _ => core.SRNull
      }
    }

    def synthesizeStreamerInfo(
      b: TBranchElement, 
      streamerInfo: TStreamerInfo,
      streamerElement: TStreamerElement, 
      parentType: core.SRTypeTag,
      flattenable: Boolean = false // is this branch flattenable
    ): core.SRType = {
      def shuffleStreamerInfo(sinfo: TStreamerInfo) = {
        val elems = sinfo.getElements
        val bases = 
          for (i <- 0 until elems.size; se=elems.get(i).asInstanceOf[TStreamerElement]
            if se.getType==0) 
            yield se
        val rest = 
          for (i <- 0 until elems.size; se=elems.get(i).asInstanceOf[TStreamerElement]
            if se.getType != 0) 
            yield se
        rest ++ bases
      }

      val elements = streamerInfo.getElements
      if (elements.size==0) // that is some empty class
        core.SRNull
      else if (elements.get(0).asInstanceOf[TStreamerElement].getName=="This") 
        synthesizeStreamerElement(b, elements.get(0).asInstanceOf[TStreamerElement],
          parentType)
      else {
        if (b==null) {
          core.SRComposite(
            if (streamerElement==null) "" else streamerElement.getName
            , null,
            for (i <- 0 until elements.size)
              yield synthesizeStreamerElement(null, 
                elements.get(i).asInstanceOf[TStreamerElement], core.SRCompositeType),
            false, false, 
            if (streamerElement == null)  false
            else if (streamerElement.getType==0)true 
            else false
          )
        }
        else if (b.getBranches.size==0) {
          // unsplittable branch
          // members do not need the branch for reading
          // buffer will be passed to them
          core.SRComposite(b.getName, b,
            for (i <- 0 until elements.size) 
              yield synthesizeStreamerElement(null, 
                elements.get(i).asInstanceOf[TStreamerElement], core.SRCompositeType),
            false,
            parentType match {
              case core.SRRootType => true
              case _ => false
            }
          )
        }
        else 
          // splittable
          // can be flattenable or not flattenable
          if (b.getType==1 || b.getType==2 || b.getType==4) {
            // this is either a BASE/Object inside some leaf
            // or an STL Collection
            synthesizeFlattenable(b, streamerInfo)
          }
          else {
            // non-flattenable branch
            core.SRComposite(
              b.getName, b,
              for (i <- 0 until b.getBranches.size; 
                sub=b.getBranches.get(i).asInstanceOf[TBranchElement])
                yield synthesizeStreamerElement(sub, 
                elements.get(sub.getID).asInstanceOf[TStreamerElement], 
                core.SRCompositeType),
              true,
              parentType match {
                case core.SRRootType => true
                case _ => false
              }
            )
          }
      }
    }

    /**
     * Synthesize a branch whose sub branches are flattened
     * @return SRType
     */
    def synthesizeFlattenable(
      b: TBranchElement, // branch whose subs are flattened
      streamerInfo: TStreamerInfo // streamer Info of this branch
    ) = {
      def findBranch(objectName: String, 
        history: Seq[String]
      ): TBranchElement = {
        // build the full name of the branch
        val fullName = 
          if (b.getType==1) 
            if (b.getName.count(_ == '.') == 0)
              // no . seps
              (history ++ Seq(objectName)).mkString(".")
            else
              // there are dots
              (b.getName.split('.').dropRight(1) ++ (history ++ Seq(objectName))).
              mkString(".")
          else
            if (b.getName.count(_ == '.') == 0)
              // no . seps
              (Seq(b.getName) ++ (history ++ Seq(objectName))).mkString(".")
            else
              (b.getName.split('.') ++ (history ++ Seq(objectName))).mkString(".")

        // debug 
        if (debug>0){
          println(s"History: $history")
          println(s"object Name: $objectName")
          println(s"fullName: $fullName")
        }

        // iterate over all of them and take the head of the result
        (for (i <- 0 until b.getBranches.size; 
          sub=b.getBranches.get(i).asInstanceOf[TBranchElement]; subName= {
            // when we have arrays, the square brackets are reflected in the name
            // of the branch - strip them!
            if (sub.getName.indexOf('[')>0) 
              sub.getName.substring(0, sub.getName.indexOf('['))
            else 
              sub.getName
          }
          if subName == fullName) yield sub).head
      }

      def iterate(info: TStreamerInfo, history: Seq[String]): Seq[core.SRType] = {
        // right away we have composite
        for (i <- 0 until info.getElements.size; 
          streamerElement=info.getElements.get(i).asInstanceOf[TStreamerElement]) yield {
          val ttt = streamerElement.getType
          if (ttt == 0) { 
            // this is the BASE class
            if (b.getType==4) {
              // STL node - everything is flattened

              // find the streamer
              val sInfo = streamers.applyOrElse(streamerElement.getName,
                (x: String) => null)
              // create a composite and recursively iterate the sub branches
              core.SRComposite(streamerElement.getName, null,
                iterate(sInfo, history), true, false)
            }
            else {
              // not an STL node

              // find the sub branch for this
              val sub = findBranch(streamerElement.getName, history)
              // find the TStreamerInfo for this object
              val sInfo = streamers.applyOrElse(streamerElement.getName,
                (x: String) => null)
              // synthesize this guy
              synthesizeStreamerInfo(sub, sInfo, streamerElement, 
                core.SRCompositeType, true)
            }
          }
          else if (ttt < 61 || ttt == 500) {
            // basic type or anything that is of STL type goes into 
            // element synthesis

            // find the branch for it!
            val sub = findBranch(streamerElement.getName, history)
            // send for synthesis
            synthesizeStreamerElement(sub, streamerElement,
              core.SRCompositeType)
          }
          else {
            // this typically would be some composite class
            // which we send recursively to itearte over again if it's not split
            // or we get the streamerInfo and send to synthesize

            // find the streamer 
            val sInfo = streamers.applyOrElse(streamerElement.getTypeName,
              (x: String) => null)

            // TODO: This try/catch is not the best
            // create the composite and recursively iterate over all the members
            // if it fails => throws an exception, check then
            // that this branch of type 2 is not actually flattenend
            try {
              core.SRComposite(streamerElement.getName, null,
                iterate(sInfo, history :+ streamerElement.getName), true, false
              )
            } catch {
              case _ : Throwable => {
                val sub = findBranch(streamerElement.getName, history)
                synthesizeStreamerInfo(sub, sInfo, streamerElement,
                  core.SRCompositeType, false)
              }
            }
          }
        }
      }

      if (debug>0) println(s"Starting synthesize of Flattenable branch: ${b.getName}")

      // create a composite by iterating recursively over the members
      core.SRComposite(b.getName, b,
        iterate(streamerInfo, Seq()), true, false)
    }

    /**
     * Map the branch => SRType
     */
    def synthesizeBranchElement(b: TBranchElement, // top branch or sub
      streamerElement: TStreamerElement, // streamer Element for a subbranch
      parentType: core.SRTypeTag
      ): core.SRType = {
      val subs = b.getBranches
      if (streamerElement==null) {
        // top branch
        core.SRNull // should not be the case
      }
      else synthesizeStreamerElement(b, streamerElement, parentType)
    }

    requiredColumns match {
      // for the initialization stage - all the columns to be mapped
      case null => new core.SRRoot(tree.getName,
        tree.getEntries,
        for (i <- 0 until tree.getNBranches; b=tree.getBranch(i))
          yield synthesizeTopBranch(b)
      )
      // for the cases like count.... 
      case Array() => new core.SREmptyRoot(tree.getName, tree.getEntries)
      // for the non-empty list of columns that are required by for a query
      case _ => new core.SRRoot(tree.getName, tree.getEntries,
        for (i <- 0 until tree.getNBranches; b=tree.getBranch(i) 
          if requiredColumns.contains(b.getName()))
          yield synthesizeTopBranch(b)
      )
    }
  }

  /**
   * @return - returns the AbstractSchemaTree RootNode
   */
  def buildAST(tree: TTree, 
    streamers: Map[String, TStreamerInfo], // map of streamers
    requiredColumns: Array[String] // list of column names that must be preserved
    ): AbstractSchemaTree = 
  {
    def synthesizeBranch(branch: TBranch): AbstractSchemaTree = 
    {
      val subs = branch.getBranches
      if (subs.size>0)
      {
        //  
        // complex node
        // TODO: Do we have these cases or TBranch has only Leaves???
        //
        null
      }
      else
      {
        //
        //  simple node - assume 1 leaf only for now
        //  1. extract the information you need(name ,title, classname, datatype)
        //  2. Assign the iterator
        //
        val mytype = assignBranchType(branch)
        val leaves = branch.getLeaves
        if (leaves.size==1)
        {
          val leaf = leaves.get(0).asInstanceOf[TLeaf]
          new TerminalNode(new Leaf(new LeafInfo(
            leaf.getName, leaf.getRootClass.getClassName, leaf.getLen
            )), new NodeInfo(
              branch.getName, branch.getTitle, branch.getRootClass.getClassName, mytype
            ), mytype.getIterator(branch).asInstanceOf[BasicBranchIterator])
        }
        else
          new TerminalMultiLeafNode(
            for (i <- 0 until leaves.size; l=leaves.get(i).asInstanceOf[TLeaf]) yield
              new Leaf(new LeafInfo( l.getName, l.getRootClass.getClassName, l.getLen
              )), new NodeInfo(
                branch.getName, branch.getTitle, branch.getRootClass.getClassName,
                mytype
              ), mytype.getIterator(branch).asInstanceOf[StructBranchIterator]
          )
      }
    }

    // synthesize the BranchElement like branches
    def synthesizeBranchElement(branchElement: TBranchElement): AbstractSchemaTree = 
    {
      val subs = branchElement.getBranches
      branchElement.getType match {
        case -1 => // TODO: mark as unknown for now
          if (subs.size==0) {
            val leaf = branchElement.getLeaves.get(0).asInstanceOf[TLeafElement]
            new UnknownTerminalNode(
              new LeafElement(new LeafElementInfo(leaf.getName,
                leaf.getRootClass.getClassName, leaf.getLen, leaf.getType)),
              new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                branchElement.getRootClass.getClassName, SRNull,
                branchElement.getParentName, branchElement.getStreamerType,
                branchElement.getType, branchElement.getClassName,
                branchElement.getID),
              null
            )
          }
          else {
            val leaf = branchElement.getLeaves.get(0).asInstanceOf[TLeafElement]
            new UnknownNode(
              for (i <- 0 until subs.size; sub=subs.get(i).asInstanceOf[TBranchElement])
                yield synthesizeBranchElement(sub),
              new LeafElement(new LeafElementInfo(leaf.getName,
                leaf.getRootClass.getClassName, leaf.getLen, leaf.getType)),
              new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                branchElement.getRootClass.getClassName, SRNull,
                branchElement.getParentName, branchElement.getStreamerType,
                branchElement.getType, branchElement.getClassName, 
                branchElement.getID)
            )
          }
        case 0 => { // LeafNode
          if (branchElement.getID<0) // unsplit object with default streamer at wt
            // type=0 and id<0 - unsplit object with default streamer at the time
            // of writting
            if (subs.size==0) 
            {
              val leaf = branchElement.getLeaves.get(0).asInstanceOf[TLeafElement]
              // need to decide if it's a collection or a an object
              // check the streamer info for this class name
              val myStreamer = streamers.applyOrElse(branchElement.getClassName,
                (x: String) => null)
              if (myStreamer!=null) {
                if (myStreamer.getElements.size==1 && 
                  myStreamer.getElements.get(0).asInstanceOf[TStreamerElement].getName=="This"
                ) // this streamerInfo is a singleton with the first element as its type
                {
                  val streamerElement = 
                    myStreamer.getElements.get(0).asInstanceOf[TStreamerElement]
                  streamerElement.getRootClass.getClassName match {
                    case "TStreamerSTL" => { 
                      new UnSplittableCollectionNodeElement(
                        new LeafElement(new LeafElementInfo(leaf.getName,
                          leaf.getRootClass.getClassName, leaf.getLen, leaf.getType)),
                        new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                          branchElement.getRootClass.getClassName, SRNull,
                          branchElement.getParentName, branchElement.getStreamerType,
                          branchElement.getType, branchElement.getClassName,
                          branchElement.getID),
                        streamerElement.asInstanceOf[TStreamerSTL],
                        null
                      )
                    }
                    case _ => null
                  }
                }
                else // this streamerINfo is for a composite object
                  new UnSplittableObjectNodeElement(
                    new LeafElement(new LeafElementInfo(leaf.getName,
                      leaf.getRootClass.getClassName, leaf.getLen, leaf.getType)),
                    new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                      branchElement.getRootClass.getClassName, SRNull,
                      branchElement.getParentName, branchElement.getStreamerType,
                      branchElement.getType, branchElement.getClassName,
                      branchElement.getID),
                    for (i <- 0 until myStreamer.getElements.size;
                      s=myStreamer.getElements.get(i).asInstanceOf[TStreamerElement])
                      yield s,
                    null
                  )
              }
              else
                new UnknownTerminalNode(
                  new LeafElement(new LeafElementInfo(leaf.getName,
                    leaf.getRootClass.getClassName, leaf.getLen, leaf.getType)),
                  new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                    branchElement.getRootClass.getClassName, SRNull,
                    branchElement.getParentName, branchElement.getStreamerType,
                    branchElement.getType, branchElement.getClassName,
                    branchElement.getID),
                  null
                )
            }
            else { // id<0 and there are subs - splittable object
              val leaf = branchElement.getLeaves.get(0).asInstanceOf[TLeafElement]
              new SplittableObjectNodeElement(
                for (i <- 0 until subs.size; sub=subs.get(i).asInstanceOf[TBranchElement])
                  yield synthesizeBranchElement(sub),
//                new LeafElement(new LeafElementInfo(leaf.getName,
//                  leaf.getRootClass.getClassName, leaf.getLen, leaf.getType)),
                new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                  branchElement.getRootClass.getClassName, SRNull,
                  branchElement.getParentName, branchElement.getStreamerType,
                  branchElement.getType, branchElement.getClassName,
                  branchElement.getID)
              )
            }
          else // simple data member of split object
          {
            val leaf = branchElement.getLeaves.get(0).asInstanceOf[TLeafElement]
            new TerminalSimpleNodeElement(
              new LeafElement(new LeafElementInfo(leaf.getName,
                leaf.getRootClass.getClassName, leaf.getLen, leaf.getType)),
              new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                branchElement.getRootClass.getClassName, SRNull,
                branchElement.getParentName, branchElement.getStreamerType,
                branchElement.getType, branchElement.getClassName,
                branchElement.getID),
              null
            )
          }
        }
        case 1 => // Base Class of a split object
          if (subs.size==0) null // are we inheriting from a simple type?
          else {
            new SplittableObjectNodeElement(
              for (i <- 0 until subs.size; sub=subs.get(i).asInstanceOf[TBranchElement])
                yield synthesizeBranchElement(sub),
              new NodeElementInfo(branchElement.getName, branchElement.getTitle,
                branchElement.getRootClass.getClassName, SRNull,
                branchElement.getParentName, branchElement.getStreamerType,
                branchElement.getType, branchElement.getClassName,
                branchElement.getID)
            )
          }
        case 2 => { // class-typed data member of a split object 
          // TODO: assume that this object is splittable for now
          new SplittableObjectNodeElement(
            for (i <- 0 until subs.size; sub=subs.get(i).asInstanceOf[TBranchElement])
              yield synthesizeBranchElement(sub),
            new NodeElementInfo(branchElement.getName, branchElement.getTitle,
              branchElement.getRootClass.getClassName, SRNull,
              branchElement.getParentName, branchElement.getStreamerType,
              branchElement.getType, branchElement.getClassName,
              branchElement.getID)
          )
        }
        case 4 => {
          val leaf = branchElement.getLeaves.get(0).asInstanceOf[TLeafElement]
          new SplittableCollectionNodeElement(
            for (i <- 0 until subs.size; sub=subs.get(i).asInstanceOf[TBranchElement])
              yield synthesizeBranchElement(sub),
            new LeafElement(new LeafElementInfo(leaf.getName,
              leaf.getRootClass.getClassName, leaf.getLen,
              leaf.getType)),
            new NodeElementInfo(branchElement.getName, branchElement.getTitle,
              branchElement.getRootClass.getClassName, SRNull,
              branchElement.getParentName, branchElement.getStreamerType,
              branchElement.getType, branchElement.getClassName,
              branchElement.getID)
          )
        }
        case 41 => {
          val leaf = branchElement.getLeaves.get(0).asInstanceOf[TLeafElement]
          new TerminalCollectionMemberNodeElement(
            new LeafElement(new LeafElementInfo(leaf.getName,
              leaf.getRootClass.getClassName, leaf.getLen,
              leaf.getType)),
            new NodeElementInfo(branchElement.getName, branchElement.getTitle,
              branchElement.getRootClass.getClassName, SRNull,
              branchElement.getParentName, branchElement.getStreamerType,
              branchElement.getType, branchElement.getClassName,
              branchElement.getID),
            null
          )
        }
        case _ => null
      }
    }

    def synthesize(branch: TBranch): AbstractSchemaTree = 
    {
      if (branch.isInstanceOf[TBranchElement])
        synthesizeBranchElement(branch.asInstanceOf[TBranchElement])
      else
        synthesizeBranch(branch)
    }

    requiredColumns match {
      // for the initialization stage - all the columns to be mapped
      case null => new RootNode(tree.getName,
        for (i <- 0 until tree.getNBranches; b=tree.getBranch(i))
          yield synthesize(b)
      )
      // for the cases like count.... 
      case Array() => new EmptyRootNode(tree.getName, tree.getEntries)
      // for the non-empty list of columns that are required by for a query
      case _ => new RootNode(tree.getName,
        for (i <- 0 until tree.getNBranches; b=tree.getBranch(i) 
          if requiredColumns.contains(b.getName()))
          yield synthesize(b)
      )
    }
  }

  /**
   * @return Spark DataFrame Schema
   */
  def buildSparkSchema(ast: AbstractSchemaTree): StructType =
  {
    def iterate(node: AbstractSchemaTree): StructField = node match {
      case Node(subnodes, info) => StructField(info.name, StructType(
        for (x <- subnodes) yield iterate(x)
      ))
      case TerminalNode(leaf, info, iter) => StructField(info.name,
        info.myType.toSparkType)
      case TerminalMultiLeafNode(leaves, info, iter) => StructField(info.name,
        info.myType.toSparkType
      )
      /*
      case NodeElement(subnodes, info) => StructField(info.name, StructType(
        for (x <- subnodes) yield iterate(x)
      ))
      case TerminalNodeElement(leaf, info, iter) => null
      */
      case _ => null
    }
    
    ast match {
      case RootNode(_, nodes) => StructType(
        for (x <- nodes) yield iterate(x)
      )
      case EmptyRootNode(_, _) => StructType(Seq())
      case _ => null
    }
  }

  /**
   * @return Spark DataFrame 1 Row
   */
  def buildSparkRow(ast: AbstractSchemaTree): Row = 
  {
    def iterate(node: AbstractSchemaTree): Any = node match {
      case Node(subnodes, info) => Row.fromSeq(
        for (x <- subnodes) yield iterate(x)
      )
      case TerminalNode(leaf, info, iter) => iter.next
      case TerminalMultiLeafNode(_, info, iter) => Row.fromSeq(iter.next)

      /*
      case NodeElement(subnodes, info) => Row.fromSeq(
        for (x <- subnodes) yield iterate(x)
      )
      case TerminalNodeElement(leaf, info, iter) => iter.next
      */
      case _ => null
    }
    
    ast match {
      case RootNode(_, nodes) => Row.fromSeq(
        for (x <- nodes) yield iterate(x)
      )
      case EmptyRootNode(_, _) => {ast.asInstanceOf[EmptyRootNode].entries-=1; Row();}
    }
  }

  /**
   * @return - void
   * prints the Tree
   */
  def printAST(ast: AbstractSchemaTree): Unit = 
  {
    def __print__(node: AbstractSchemaTree, level: Int): Unit = node match 
    {
      case RootNode(name, nodes) => {
        println(name)
        for (x <- nodes) __print__(x, level+2)
      }
      case EmptyRootNode(name, entries) => println(name + " Entries=" + entries)
      case Node(subnodes, info) => {
        println(("  "*level) + info)
        for (x <- subnodes) __print__(x, level+2)
      }
      case TerminalNode(leaf, info, iter) => 
        println(("  "*level) + info + " ---> " + leaf.info)
      case TerminalMultiLeafNode(leaves, info, iter) => {
        println(("  "*level) + info + " ---> " + leaves.map(_.info.toString).mkString(" :: "))
      }
      // Splittable Collection Node - 1 level of nestedness
      case SplittableCollectionNodeElement(subnodes, leaf, info) => {
        println(("  "*level) + "Splittable Collection :: "+ info + " ---> " + leaf.info)
        for (sub <- subnodes) __print__(sub, level+2)
      }
      case TerminalCollectionMemberNodeElement(leaf, info, _) => {
        println(("  "*level)+ "Collection Member :: " + info + " ---> " + leaf.info)
      }
      case SplittableObjectNodeElement(subnodes, info) => {
        println(("  "*level) + "Splittable Object :: " + info + " ---> no leaf")
        for (sub <- subnodes) __print__(sub, level+2)
      }
      case UnSplittableCollectionNodeElement(leaf, info, streamer, iter) => {
        println(("  "*level) + "UnSplittable Collection :: " + 
          info + " ---> " + leaf.info)
      }
      case UnSplittableObjectNodeElement(leaf, info, streamers, iter) => {
        println(("  "*level) + "UnSplittable Object :: " + info + " ---> " + leaf.info)
      }
      case TerminalSimpleNodeElement(leaf, info, _) => 
        println(("  "*level) + "Terminal Simple :: "+ info + " ---> " + leaf.info)
      case TerminalObjectNodeElement(leaf, info, _) => 
        println(("  "*level) + "Terminal Object :: " + info + " ---> " + leaf.info)

      case UnknownNode(subnodes, leaf, info) => {
        println(("  "*level) + info + " ---> " + leaf.info)
        for (sub <- subnodes) __print__(sub, level+2)
      }
      case UnknownTerminalNode(leaf, info, iter) => 
        println(("  "*level) + info + " ---> " + leaf.info)
      case _ => println(null)
    }

    __print__(ast, 0)
  }

  def containsNext(ast: AbstractSchemaTree): Boolean = ast match {
    case RootNode(name, nodes) => containsNext(nodes.head)
    case EmptyRootNode(name, entries) => entries>0
    case Node(subnodes, info) => containsNext(subnodes head)
    case TerminalNode(leaf, info, iter) => iter.hasNext
    case TerminalMultiLeafNode(_, _, iter) => iter.hasNext

    /*
    case NodeElement(subnodes, info) => containsNext(subnodes head)
    case TerminalNodeElement(leaf, info, iter) => false
    */
    
    case _ => false
  }

  /*
   * Section for some utils
   */
  def findTree(dir: TDirectory): TTree = // find the Tree
  {
    for (i <- 0 until dir.nKeys) {
      val obj = dir.getKey(i).getObject.asInstanceOf[AbstractRootObject]
      if (obj.getRootClass.getClassName == "TDirectory" ||
        obj.getRootClass.getClassName == "TTree") 
      {
        if (obj.getRootClass.getClassName == "TDirectory")
          return findTree(obj.asInstanceOf[TDirectory])
        else (obj.getRootClass.getClassName == "TTree")
        return obj.asInstanceOf[TTree]
      }
    }
    null
  }


  def arrangeStreamers(reader: RootFileReader): Map[String, TStreamerInfo] = 
  {
    val streamers = reader.streamerInfo
    (for (i <- 0 until streamers.size; s=streamers.get(i) 
      if s.isInstanceOf[TStreamerInfo]; streamer=s.asInstanceOf[TStreamerInfo])
      yield (streamer.getName, streamer)
    ).toMap
  }
}
