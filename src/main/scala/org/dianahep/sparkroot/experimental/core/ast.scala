package org.dianahep.sparkroot.experimental

import org.dianahep.sparkroot.core._
import org.dianahep.root4j.interfaces._
import org.dianahep.root4j.core._
import org.dianahep.root4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

package object core
{
  private lazy val logger = LogManager.getLogger("org.dianahep.sparkroot.core.ast") 

  /**
   * @return prints the Abstractly Typed Tree
   */
  def printATT(att: core.SRType, level: Int = 0, sep: String = "  "): String = att match {
    case core.SRNull(_) => sep*level+"Null" + "\n"
    case core.SRUnknown(name, _) => sep*level + s"$name: Unknown" + "\n"
    case core.SRRoot(name, entries, types) => {
      s"Root: $name wtih $entries Entries" + "\n" + (for (t <- types) 
        yield printATT(t, level+1)).mkString("")
    }
    case core.SREmptyRoot(name, entries) =>
      s"Empty Root: $name with $entries Entries" + "\n"
    case core.SRInt(name, _, _, _) => sep*level+s"$name: Integer" + "\n"
    case core.SRString(name, _, _, _) => sep*level+s"$name: String" + "\n"
    case core.SRLong(name, _, _, _) => sep*level+s"$name: Long" + "\n"
    case core.SRDouble(name, _, _, _) => sep*level+s"$name: Double" + "\n"
    case core.SRByte(name, _, _, _) => sep*level+s"$name: Byte" + "\n"
    case core.SRBoolean(name, _, _, _) => sep*level+s"$name: Boolean" + "\n"
    case core.SRFloat(name, _, _, _) => sep*level+s"$name: Float" + "\n"
    case core.SRShort(name, _, _, _) => sep*level + s"$name: Short" + "\n"
    case core.SRArray(name, _, _, t, n, _) => {
      sep*level + s"$name: Array[$n]" + "\n" + printATT(t, level+1)
    }
    case core.SRVector(name, _, t, split, isTop, _) => {
      sep*level + s"$name: STL Vector. split=$split and isTop=$isTop" + "\n" + printATT(t, level+1)
    }
    case core.SRMap(name, _, keyType, valueType, split, isTop, _) => {
      sep*level + s"$name: Map ${keyType.name} => ${valueType.name}. split=$split and isTop=$isTop" + "\n" + sep*(level+1) + "Key Type:" + "\n" + printATT(keyType, level+2) + 
        sep*(level+1) + "Value Type:" + "\n" + printATT(valueType, level+2)
    }
    case core.SRSTLString(name, _, isTop, _) => {
      sep*level + s"$name: STL String isTop=$isTop" + "\n"
    }
    case core.SRComposite(name, b, members, split, isTop, isBase, _) => {
      sep*level + s"${name}: Composite split=$split isTop=$isTop isBase=$isBase" + "\n" +
        (for (t <- members) yield printATT(t, level+1)).mkString("")
    }
    case _ => ""
  }

  def buildSparkSchema(att: core.SRType) = att.toSparkType.asInstanceOf[StructType]
  def readSparkRow(att: core.SRType): Row = att.read.asInstanceOf[Row]
  def containsNext(att: core.SRType) = att.hasNext

  /**
   * Build ATT - Abractly Typed Tree
   *
   * @return ATT
   */
  def buildATT(
    tree: TTree,
    streamers: Map[String, TStreamerInfo],
    requiredSchema: Option[StructType]
  ): core.SRType = {

    def synthesizeLeafType(b: TBranch, leaf: TLeaf): core.SRType = {
      // TODO: this is a hack!!! Naming Conventions should be identified properly!!!
      // nameToUse should be fixed!
      val nameToUse = if (b.getLeaves.size==1) b.getName else leaf.getName
      leaf.getRootClass.getClassName.last match {
        case 'C' => core.SRString(nameToUse, b, leaf)
        case 'B' => core.SRByte(nameToUse, b, leaf)
        case 'b' => core.SRByte(nameToUse, b, leaf)
        case 'S' => core.SRShort(nameToUse, b, leaf)
        case 's' => core.SRShort(nameToUse, b, leaf)
        case 'I' => core.SRInt(nameToUse, b, leaf)
        case 'i' => core.SRInt(nameToUse, b, leaf)
        case 'F' => core.SRFloat(nameToUse, b, leaf)
        case 'D' => core.SRDouble(nameToUse, b, leaf)
        case 'L' => core.SRLong(nameToUse, b, leaf)
        case 'l' => core.SRLong(nameToUse, b, leaf)
        case 'O' => core.SRBoolean(nameToUse, b, leaf)
        case _ => core.SRNull()
      }
    }

    def synthesizeLeaf(b: TBranch, leaf: TLeaf): core.SRType = {
      val nameToUse = if (b.getLeaves.size==1) b.getName else leaf.getName
      def iterate(dimsToGo: Int): core.SRType = {
        if (dimsToGo==1) core.SRArray(
          // TODO: this is a hack!!! Naming Conventions should be identified properly!!!
          // nameToUse should be fixed!
          nameToUse, b, leaf, synthesizeLeafType(b, leaf), 
          leaf.getMaxIndex()(leaf.getArrayDim-1))
        else
          // TODO: this is a hack!!! Naming Conventions should be identified properly!!!
          // nameToUse should be fixed!
          core.SRArray(nameToUse, b, leaf, iterate(dimsToGo-1), leaf.getMaxIndex()(
            leaf.getArrayDim-dimsToGo))
      }

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
      return core.SRNull();
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
      case 12 =>  core.SRShort("", null, null)
      case 13 => core.SRInt("", null, null)
      case 14 => core.SRLong("", null, null)
      case 15 => core.SRInt("", null, null)
      case 16 => core.SRLong("", null, null)
      case 17 => core.SRLong("", null, null)
      case 18 => core.SRBoolean("", null, null)
      case 19 => core.SRShort("", null, null)
      case _ => core.SRNull()
    }

    /*
     * Format the name if it's a pointer to the class name
     */
    def formatNameForPointer(className: String) = 
      if (className.last=='*') className.take(className.length-1)
      else className

    def synthesizeStreamerElement(
      b: TBranchElement, 
      streamerElement: TStreamerElement,
      parentType: core.SRTypeTag
      ): core.SRType = {

      // when you have an array of something simple kOffsetL by ROOT convention  
      def iterateArray(dimsToGo: Int): core.SRType = {
        logger.info(s"dimsToGo = ${dimsToGo} for name = ${streamerElement.getName} type = ${streamerElement.getType} typeName = ${streamerElement.getTypeName}")
        if (dimsToGo==1) core.SRArray(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement], 
          synthesizeBasicStreamerType(streamerElement.getType-20) match {
            case core.SRNull(_) => synthesizeStreamerElement(b,
              new TStreamerElement {
                def getArrayDim = streamerElement.getArrayDim
                // TODO - what does this method stand for???
                def getArrayLength = streamerElement.getArrayLength
                def getMaxIndex = streamerElement.getMaxIndex
                def getSize = streamerElement.getSize
                def getType = streamerElement.getType - 20
                def getTypeName = streamerElement.getTypeName
                def getName = streamerElement.getName
                def getTitle = streamerElement.getTitle
                def getBits = streamerElement.getBits
                def getUniqueID = streamerElement.getUniqueID
                def getRootClass = streamerElement.getRootClass
              },
              parentType
            )
            case x @ _ => x
          }
          ,
          streamerElement.getMaxIndex()(streamerElement.getArrayDim-1))
      else
        core.SRArray(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement], 
          iterateArray(dimsToGo-1), streamerElement.getMaxIndex()(
            streamerElement.getArrayDim-dimsToGo))
      }

      streamerElement.getType match {
        case 0 => { // BASE CLASS
          // assume for now that the inheritance is from composite classes
          // NOTE: get the name instead of type name for the super class
          logger.debug(s"case 0 => streamerElement.getTypeName = ${streamerElement.getTypeName} streamerElement.getName = ${streamerElement.getName}")
          val streamerInfo = streamers.applyOrElse(streamerElement.getName,
          (x: String) => null)
          if (streamerInfo==null) core.SRUnknown(streamerElement.getName)
          else {
            // in principle there must be the TStreamerInfo for all the bases 
            // used
            logger.debug(s"There is a class name for: ${streamerElement.getName}")
            if (streamerInfo.getElements.size==0) 
              // empty BASE CLASS, create an empty composite
              // splitting does not matter - it's empty
            // BUT it's the base class!!!
              core.SRComposite(streamerElement.getName, b, Seq(), false, false, true)
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
        case it if 81 until 89 contains it => iterateArray(streamerElement.getArrayDim)
        case 61 => {
          // NOTE: get the type name
          val streamerInfo = streamers.applyOrElse(
            formatNameForPointer(streamerElement.getTypeName),
          (x: String) => null)
          if (streamerInfo==null) core.SRUnknown(streamerElement.getName)
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }
        case 62 => {
          // NOTE: get the typename
          val streamerInfo = streamers.applyOrElse(
            formatNameForPointer(streamerElement.getTypeName),
            (x: String) => null)
          if (streamerInfo==null) {
            val isCustom = tryCustom().applyOrElse(
              formatNameForPointer(streamerElement.getTypeName), 
              (x: String) => core.SRNull())
            if (!isCustom.isInstanceOf[core.SRNull]) isCustom
            else core.SRUnknown(streamerElement.getName)
          }
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }

        // TODO: Retrieiving of TObject derived classes is not supported yet
        // TObject
        case 66 => {
          // NOTE: get the typename
          logger.debug(s"case 66 => streamerElement.getTypeName = ${streamerElement.getTypeName} streamerElement.getName = ${streamerElement.getName}")
          val streamerInfo = streamers.applyOrElse(
            if (streamerElement.getTypeName == "BASE")
              streamerElement.getName
            else 
              streamerElement.getTypeName, (x: String) => null)
          if (streamerInfo==null) core.SRUnknown(streamerElement.getName)
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }
        case 67 => {
          // NOTE: get the typename
          val streamerInfo = streamers.applyOrElse(streamerElement.getTypeName,
            (x: String) => null)
          if (streamerInfo==null) core.SRUnknown(streamerElement.getName)
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }
        // TString
        case 65 => core.SRString(streamerElement.getName, b,
          if (b==null) null
          else b.getLeaves.get(0).asInstanceOf[TLeafElement])
        case 500 => synthesizeStreamerSTL(b, streamerElement.asInstanceOf[TStreamerSTL],
          parentType)
        case 69 => {
          logger.debug(s"typeName=${streamerElement.getTypeName} name=${streamerElement.getName} strippedName=${streamerElement.getTypeName.take(streamerElement.getTypeName.length-1)}")
          // this is a pointer
          val streamerInfo = streamers.applyOrElse(
            formatNameForPointer(streamerElement.getTypeName),
            (x: String) => null)
          if (streamerInfo == null) core.SRUnknown(streamerElement.getName)
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }
        case 63 => {
          logger.debug(s"typeName=${streamerElement.getTypeName} name=${streamerElement.getName} strippedName=${streamerElement.getTypeName.take(streamerElement.getTypeName.length-1)}")
          // this is a pointer
          val streamerInfo = streamers.applyOrElse(
            formatNameForPointer(streamerElement.getTypeName),
            (x: String) => null)
          if (streamerInfo == null) core.SRUnknown(streamerElement.getName)
          else synthesizeStreamerInfo(b, streamerInfo, streamerElement, parentType)
        }
        case _ => core.SRUnknown("unidentified STreamerElement type")
      }
    }

    /**
     * @return the full type definition for a basic type
     *
     * Array is also a basic type - leave it out for now
     */
    def synthesizeBasicTypeName(
      typeName: String, // basic type name
      objName: String = ""
    ): core.SRType = {

      typeName match {
        case "int" => core.SRInt(objName, null, null)
        case "float" => core.SRFloat(objName, null, null)
        case "double" => core.SRDouble(objName, null, null)
        case "char" => core.SRByte(objName, null, null)
        case "long" => core.SRLong(objName, null, null)
        case "short" => core.SRShort(objName, null, null)
        case "bool" => core.SRBoolean(objName, null, null)
        case "unsigned int" => core.SRInt(objName, null, null)
        case "unsigned char" => core.SRByte(objName, null, null)
        case "unsigned long" => core.SRLong(objName, null, null)
        case "unsigned short" => core.SRShort(objName, null, null)

        // ROOT ones ending with t
        case "Double32_t" => core.SRFloat(objName, null, null)
        case _ => core.SRNull()
      }
    }

    /**
     * @return - list of template arguments
     * c++ Template Parenthesis balancing
     */
    def extractTemplateArguments(fullTemplateString: String): Seq[String] = {
      def iterate(n: Int, from: Integer, 
                  currentPos: Integer, acc: Seq[String]): Seq[String] = 
        if (currentPos==fullTemplateString.length)
          acc :+ fullTemplateString.substring(from)
        else if (fullTemplateString(currentPos)==',')
          if (n==0) // only if parenthesese are balanaced
            iterate(0, currentPos+1, currentPos+1, 
              acc:+fullTemplateString.substring(from, currentPos))
          else 
            iterate(n, from, currentPos+1, acc)
        else if (fullTemplateString(currentPos)=='<')
          iterate(n+1, from, currentPos+1, acc)
        else if (fullTemplateString(currentPos)=='>')
          iterate(n-1, from, currentPos+1, acc)
        else
          iterate(n, from, currentPos+1, acc)

      iterate(0, 0, 0, Seq[String]())
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
     * 4) enum
     *
     * we basically assume that if something is not present, it must be something of STL
     */
    def synthesizeClassName(
      className: String, // c++ standard class type declaration (w/ spaces for templ.)
      b: TBranchElement, // if the branch is split, we still need it
      parentType: core.SRTypeTag, // the tag for what our parent is
      objName: String = "" // the name of the object being syntehsized
    ): core.SRType = {
      val stlLinear = Seq("vector", "list", "deque", "set", "multiset",
        "forward_list", "unordered_set", "unordered_multiset")
      val stlAssociative = Seq("map", "unordered_map", "multimap", "unordered_multimap")
      val stlPair = "pair"
      val stlBitset = "bitset"
      val stlStrings = Seq("string", "__basic_string_common<true>")
      
      // quickly parse the class type and template argumetns
      val classTypeRE = "(.*?)<(.*?)>".r
      logger.debug(s"classType being synthesized: ${className} ${className.trim.length} ${className.length}")
      val (classTypeString, argumentsTypeString) = className match {
        case classTypeRE(aaa,bbb) => (aaa,bbb.trim)
        case _ => (null, null)
      }

      logger.debug(s"Parsed classType=$classTypeString and argumentsType=$argumentsTypeString")

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

      // 
      // check if it's among custom streamers
      //
      val isCustom = tryCustom(if (b == null) objName else b.getName).applyOrElse(className,
        (x: String) => core.SRNull())
      if (!isCustom.isInstanceOf[core.SRNull]) return isCustom

      // if parsing is unsuccessful, assign null
      if (classTypeString == null || argumentsTypeString == null)
        return core.SRUnknown(className)

      // based on the class type name
      classTypeString match {
        case it if stlBitset == it => {
          // remap this guy to vector of bool
          synthesizeClassName("vector<bool>", b, parentType, objName)
        }
        case it if stlLinear contains it => {
          // we have something that is vector like
          // arguments must be a single typename
          // 1. check if it's a basic type name
          val templateArguments = extractTemplateArguments(argumentsTypeString)
          val valueTypeName = 
            if (templateArguments.length>0) templateArguments(0)
            else argumentsTypeString
          val streamerInfo = streamers.applyOrElse(
            formatNameForPointer(valueTypeName),
            (x: String) => null)
          val valueType = 
            if (streamerInfo == null) {
              // no streamer info for value type
              // is it a basic type
              // else synthesize the name again
              val basicType = synthesizeBasicTypeName(valueTypeName)
              if (basicType.isInstanceOf[core.SRNull])
                // not a basic type
                // can not be composite class - must have a TStreamerInfo
                // should be some STL - nested => no subbranching
                synthesizeClassName(valueTypeName, null,
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
              case _ => core.SRVector(objName, b, valueType, false, true)
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
          logger.debug(s"Synthesizing the current class arguments: ${classTypeString}")
          val isMulti = classTypeString.contains("multi")
          val templateArguments = extractTemplateArguments(argumentsTypeString)
          val keyTypeString = 
            if (templateArguments.length>=2) templateArguments(0)
            else null
          val valueTypeString = 
            if (templateArguments.length>=2) templateArguments(1)
            else null

          // if there is a matching issue - assign null
          if (keyTypeString==null || valueTypeString==null) return core.SRNull()

          // retrieve the key Type
          val keyStreamerInfo = streamers.applyOrElse(
            formatNameForPointer(keyTypeString),
            (x: String) => null)
          val keyType = 
            if (keyStreamerInfo == null) {
              // no streamer info
              if (synthesizeBasicTypeName(keyTypeString).isInstanceOf[core.SRNull]) 
                synthesizeClassName(keyTypeString, null, core.SRCollectionType)
              else
                synthesizeBasicTypeName(keyTypeString)
            }
            else
              synthesizeStreamerInfo(null, keyStreamerInfo, null,
                core.SRCollectionType)

          // value type
          val streamerInfo = streamers.applyOrElse(
            formatNameForPointer(valueTypeString),
            (x: String) => null)
          val valueType =
            if (streamerInfo == null) {
              // no streamer info
              // is basic type
              // else synthesize the name again
              val basicType = synthesizeBasicTypeName(valueTypeString)
              if (basicType.isInstanceOf[core.SRNull])
                // not a basic type
                synthesizeClassName(valueTypeString, null,
                  core.SRCollectionType) 
              else basicType
            }
            else
              // there is a TStreamerInfo
              synthesizeStreamerInfo(b, streamerInfo, null,
                core.SRCollectionType)

          // TODO: we need to do each collection separately???
          // that is only the case when we have version for each STL separately read in
          if (b==null) 
            parentType match {
              // this is not the top collection
              case core.SRCollectionType => 
                if (isMulti)
                  new core.SRMultiMap("", b, keyType,
                    valueType, false, false)
                else
                  new core.SRMap("", b, keyType,
                    valueType, false, false)
              // this is the top collection
              case _ => 
                if (isMulti)
                  new core.SRMultiMap("", b, keyType, valueType, false, true)
                else
                  new core.SRMap("", b, keyType, valueType, false, true)
            }
          else
            parentType match {
              case core.SRCollectionType => 
                if (isMulti)
                  new core.SRMultiMap(b.getName, b, keyType,
                    valueType, false, false)
                else
                  new core.SRMap(b.getName, b, keyType,
                    valueType, false, false)
              // if there are multiple sub branches of this guy - it's split
              case _ => 
                if (isMulti)
                  new core.SRMultiMap(b.getName, b, keyType, valueType,
                    if (b.getBranches.size==0) false else true, true)
                else
                  new core.SRMap(b.getName, b, keyType, valueType,
                    if (b.getBranches.size==0) false else true, true)
            }
        }
        case it if it == stlPair => {
          // pair is considered to be the composite
          val templateArguments = extractTemplateArguments(argumentsTypeString)
          val firstTypeString = 
            if (templateArguments.length==2) templateArguments(0)
            else null
          val secondTypeString = 
            if (templateArguments.length==2) templateArguments(1)
            else null

          logger.debug(s"We got a pair: first=$firstTypeString second=$secondTypeString")

          // if there is a matching issue - assign null
          if (firstTypeString==null || secondTypeString==null) return core.SRNull()

          //  streamer info for first/second
          val streamerInfoFirst = streamers.applyOrElse(
            formatNameForPointer(firstTypeString),
            (x: String) => null)
          val streamerInfoSecond = streamers.applyOrElse(
            formatNameForPointer(secondTypeString),
            (x: String) => null)

          // get the type for first
          val firstType =
            if (streamerInfoFirst == null) {
              // no streamer info
              // is basic type
              // else synthesize the name again
              val basicType = synthesizeBasicTypeName(firstTypeString, "first")
              if (basicType.isInstanceOf[core.SRNull])
                // not a basic type
                synthesizeClassName(firstTypeString, 
                  if (b==null) null
                  else if (b.getBranches.size==0) null
                  else b.getBranches.get(0).asInstanceOf[TBranchElement],
                  core.SRCompositeType, "first") 
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
              val basicType = synthesizeBasicTypeName(secondTypeString, "second")
              if (basicType.isInstanceOf[core.SRNull])
                // not a basic type
                synthesizeClassName(secondTypeString,
                  if (b==null) null
                  else if (b.getBranches.size==0) null
                  else b.getBranches.get(1).asInstanceOf[TBranchElement],
                  core.SRCompositeType, "second") 
              else basicType
            }
            else {
              // there is a TStreamerInfo
              synthesizeStreamerInfo(null, streamerInfoSecond, null,
                core.SRCompositeType, false, "second")
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
        case _ => core.SRNull()
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
      logger.debug(s"TStreamer STL for typeName: ${streamerSTL.getTypeName} and type=${streamerSTL.getSTLtype}")

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
              val streamerInfo = streamers.applyOrElse(
                formatNameForPointer(memberClassName), 
                (x: String) => null)


              if (streamerInfo==null) 
                // no streamer info 
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
        case 6 => { // std::set.... TODO: why multimap appears as 6???
          logger.debug(s"Found type=6 ${streamerSTL.getTypeName}")
          synthesizeClassName(streamerSTL.getTypeName,
            b, parentType)
        }
        case 5 => { // std::multimap
          logger.debug(s"Found type=5 ${streamerSTL.getTypeName}")
          synthesizeClassName(streamerSTL.getTypeName,
            b, parentType)
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
        case 8 => {
          // std::bitset - map it to vector of bool
          synthesizeClassName("vector<bool>", b, parentType, streamerSTL.getName)
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
        case _ => core.SRNull()
      }
    }

    def synthesizeStreamerInfo(
      b: TBranchElement, 
      streamerInfo: TStreamerInfo,
      streamerElement: TStreamerElement, 
      parentType: core.SRTypeTag,
      flattenable: Boolean = false, // is this branch flattenable
      objName: String = "" // when both b and streamerElement are nulls => this is the name
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

      logger.debug(s"Starting StreamerInfo synthesis: ${streamerInfo.getName} numElems=${streamerInfo.getElements.size}")
      val elements = streamerInfo.getElements
      //
      // TODO: This has to be fixed properly!
      // check the recursitivity. For now a simple check...
      //
      for (i <- 0 until elements.size; x=elements.get(i).asInstanceOf[TStreamerElement]; 
        typeName=x.getTypeName) 
        if (typeName==s"vector<${streamerInfo.getName}>" ||
            typeName==s"${streamerInfo.getName}*")
          return core.SRNull()

      // regular sequence of synthesis
      if (elements.size==0) // that is some empty class
        core.SRComposite(
          if (streamerElement!=null) streamerElement.getName else s"${streamerInfo.getName}", 
          b, Seq(), false, false, 
          if (streamerElement!=null) streamerElement.getType==0 else false)
      else if (elements.get(0).asInstanceOf[TStreamerElement].getName=="This") 
        synthesizeStreamerElement(b, elements.get(0).asInstanceOf[TStreamerElement],
          parentType)
      else if (streamerInfo.getName == "TClonesArray") {
        if (b == null) {
          // only for clone that occupy a branch.
          core.SRNull()
        }else {
          // get the name of the object in the TClonesArray
          val typeName = b.getClonesName
          // create a name to be synthesized - just map to vector
          val nameToSynthesize = s"vector<$typeName>"
          // send a vector<typeName> to be synthesized
          // this will eventually call back to synthesizeStreamerInfo
          // and get properly unwrapped
          synthesizeClassName(nameToSynthesize, b, parentType)
        }
      }
      else {
        if (b==null) {
          core.SRComposite(
            if (streamerElement==null) objName else streamerElement.getName
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
          if (b.getType==1 || b.getType==2 || b.getType==3 || b.getType==4) {
            // this is either a BASE/Object inside some leaf
            // or an STL Collection or TClonesArray
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
      logger.debug(s"${streamerInfo.getName}")
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

        // some debug stmts
        logger.debug(s"History: $history")
        logger.debug(s"object Name: $objectName")
        logger.debug(s"fullName: $fullName")
        logger.debug(s"branchName: ${b.getName}")

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
        logger.debug(s"synthesizeFlattenable__iterate ${info.getElements.size} $history ${info.getName}")
        for (i <- 0 until info.getElements.size; 
          streamerElement=info.getElements.get(i).asInstanceOf[TStreamerElement]
          // skip if TObject is -1 => don't create a composite for it
          if streamerElement.getType >= 0) yield {
          logger.debug(s"StreamerElement: type=${streamerElement.getType} name=${streamerElement.getName} typeName=${streamerElement.getTypeName}")

          val ttt = streamerElement.getType
          // TODO: streamerElement.getName=="BASE" 
          // should not be there! For some reason ttt will be 66, although this the 
          // BASE class
          if (ttt == 0 || (streamerElement.getTypeName=="BASE" && ttt==66)) { 
            // this is the BASE class
            if (b.getType==4 || b.getType==3) {
              // STL node - everything is flattened
              // TClonesArray node - everything is flattened

              // find the streamer
              val sInfo = streamers.applyOrElse(streamerElement.getName,
                (x: String) => null)
              // create a composite and recursively iterate the sub branches
              // remember we are creating a BASE Composite
              core.SRComposite(streamerElement.getName, null,
                iterate(sInfo, history), true, false, true)
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
          else if (ttt < 61 || ttt == 500 || (ttt>=81 && ttt<=89)) {
            // basic type or anything that is of STL type or type with an offsetL
            // goes into element synthesis

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
            val sInfo = streamers.applyOrElse(
                formatNameForPointer(streamerElement.getTypeName),
              (x: String) => null)

            // if there is no sInfo
            if (sInfo == null) core.SRUnknown(streamerElement.getName)
            else {

              // TODO: This try/catch is not the best
              // create the composite and recursively iterate over all the members
              // if it fails => throws an exception, check then
              // that this branch of type 2 is not actually flattenend
              try {
                logger.debug(s"trying to create composite ${streamerElement.getName} $history ${sInfo.getName}")
                core.SRComposite(streamerElement.getName, null,
                  iterate(sInfo, history :+ streamerElement.getName), true, false
                )
              } catch {
                case ex : Throwable => {
                  logger.debug(s"exception " + ex.toString)
                  logger.debug(s"caught exception: for ${streamerElement.getName} ${sInfo.getName} ${info.getName}")
                  val sub = findBranch(streamerElement.getName, 
                    history)
                  synthesizeStreamerInfo(sub, sInfo, streamerElement,
                    core.SRCompositeType, false)
                }
              }
            }
          }
        }
      }

      logger.debug(s"Starting synthesize of Flattenable branch: ${b.getName}")

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
        core.SRNull() // should not be the case
      }
      else synthesizeStreamerElement(b, streamerElement, parentType)
    }

    requiredSchema match {
      // for the initialization stage - all the columns to be mapped
      case None => new core.SRRoot(tree.getName,
        tree.getEntries,
        for (i <- 0 until tree.getNBranches; b=tree.getBranch(i))
          yield synthesizeTopBranch(b)
      )
      case Some(schema) if schema.fields.size==0 => 
        new core.SREmptyRoot(tree.getName, tree.getEntries)

      // for the non-empty list of columns that are required by for a query
      case Some(schema) => new core.SRRoot(tree.getName, tree.getEntries,  
        for (rc <- schema.fields.map(_.name); i <- 0 until tree.getNBranches; 
          b = tree.getBranch(i) if b.getName().replace(".", "_")==rc)
          yield synthesizeTopBranch(b)
      )
    }
  }

  /*
   * Section for some utils
   */
  def findTree(dir: TDirectory, name: Option[String] = None): Option[TTree] = 
  {
    for (i <- 0 until dir.nKeys) {
      val key = dir.getKey(i).asInstanceOf[TKey]
      if (key.getObjectClass.getClassName == "TDirectory") 
        findTree(key.getObject.asInstanceOf[TDirectory], name) match {
          case Some(tree) => return Some(tree)
          case None => ()
        }
      else if (key.getObjectClass.getClassName == "TTree") 
        name match {
          case Some(nnn) if nnn==key.getName => 
            return Some(key.getObject.asInstanceOf[TTree])
          case None => return Some(key.getObject.asInstanceOf[TTree])
          case _ => None
        }
    }

    None
  }

  /**
   * Groups streamers by class name/type name; Identifies the ones with several streamers
   * per class name and selects the one to use
   */
  def arrangeStreamers(reader: RootFileReader): Map[String, TStreamerInfo] = 
  {
    val lll = reader.streamerInfo
    val streamers: Map[String, Seq[TStreamerInfo]] = 
      (for (i <- 0 until lll.size; s=lll.get(i) 
      if s.isInstanceOf[TStreamerInfo]; streamer=s.asInstanceOf[TStreamerInfo])
      yield (streamer)
      ).groupBy(_.getName)

    // binary op between 2 streamers to select just one 
    def selectOne(s1: TStreamerInfo, s2: TStreamerInfo): TStreamerInfo = {
      def iterate(indx: Int): TStreamerInfo = 
        if (indx == s1.getElements.size) s1
        else if (s1.getElements.get(indx).asInstanceOf[TStreamerElement].getTypeName ==
            s2.getElements.get(indx).asInstanceOf[TStreamerElement].getTypeName) 
          iterate(indx+1)
        else {
          val tn1 = streamers.applyOrElse(
              s1.getElements.get(indx).asInstanceOf[TStreamerElement]
                .getTypeName, (x: String) => null)
          val tn2 = streamers.applyOrElse(
              s2.getElements.get(indx).asInstanceOf[TStreamerElement]
                .getTypeName, (x: String) => null)

          if (tn1==null && tn2!=null) s2
          else if (tn1!=null && tn2==null) s1
          else if (tn1==null && tn2==null) iterate(indx+1) // if both are null
          else s1 // does not matter which
        }

      if (s1.getElements.size != s2.getElements.size) s1 // TODO???
      else iterate(0)
    }
    streamers.mapValues(x => if (x.size==1) x.head else x.foldLeft(x.head)(selectOne))
  }

  def tryCustom(name: String = ""): Map[String, core.SRType] = {
  val customStreamers: Map[String, core.SRType] = Map(
    "trigger::TriggerObjectType" -> core.SRInt(name, null, null),
    "reco::Muon::MuonTrackType" -> core.SRInt(name, null, null),
    "pat::IsolationKeys" -> core.SRInt(name, null, null),
    "reco::IsoDeposit" -> core.SRInt(name, null, null),
    "edm::RefCoreWithIndex" -> core.SRComposite("product_",
      null, 
      Seq(
        core.SRShort("f1", null, null),
        core.SRShort("f2", null, null),
        core.SRInt("f3", null, null),
        core.SRShort("f4", null, null)
      ),
      false, false, false
    )
  )
  return customStreamers
  }
}
