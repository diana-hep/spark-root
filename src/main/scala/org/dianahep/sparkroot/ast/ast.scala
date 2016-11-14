package org.dianahep.sparkroot

import org.dianahep.sparkroot.ast._
import org.dianahep.root4j.interfaces._
import org.dianahep.root4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

package object ast
{

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
  class NodeInfo(val name: String, val title: String, val className: String,
    val myType: SRType)
  {
    override def toString = (name, title, className, myType).toString
  }
  class NodeElementInfo(override val name: String, override val title: String, 
    override val className: String,
    override val myType: SRType,
    val streamerTypeCode: Int, val myTypeCode: Int, val objClassName: String) 
    extends NodeInfo(name, title, className, myType)
  {
    override def toString = (name, title, className, myType, streamerTypeCode, 
      myTypeCode, objClassName).toString
  }

  abstract class AbstractSchemaTree;
  case class RootNode(name: String, nodes: Seq[AbstractSchemaTree]) 
    extends AbstractSchemaTree;

  //  simple TBranch/TLeaf representations
  class Leaf(val info: LeafInfo) extends AbstractSchemaTree;
  case class TerminalNode(leaf: Leaf, info: NodeInfo, 
    iter: BranchIterator[Any]) extends AbstractSchemaTree;
  case class TerminalMultiLeafNode(leaves: Seq[Leaf], info: NodeInfo,
    iter: StructBranchIterator) extends AbstractSchemaTree;
  case class Node(subnodes: Seq[AbstractSchemaTree], info: NodeInfo) 
    extends AbstractSchemaTree;

  //  TBranchElement/TLeafElement representations
  class LeafElement(override val info: LeafElementInfo) extends Leaf(info);
  case class TerminalNodeElement(leaf: LeafElement, info: NodeElementInfo,
    iter: BranchIterator[Any]) extends AbstractSchemaTree;
  case class NodeElement(subnodes: Seq[AbstractSchemaTree], info: NodeInfo)
    extends AbstractSchemaTree;

  /**
   * for simple branches - these are the ROOT Type/Codes => our internal type system
   * @return - return the DataType representing the code
   */
  def assignTypeByLeafCode(code: Char): SRType = code match
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
   *  @return return either 1D array or MultiD Array 
   *  at this point, fixed dimensions
   */
  def checkLeafForArrayType(str: String): SRType = 
  {
    val typeCode = assignTypeByLeafCode(str.drop(str.length-1).head)

    def iterate(s: String, srtype: SRMultiArrayType): SRMultiArrayType = 
    {
      val idxstart = s.indexOf('[')
      if (idxstart > -1)
      {
        val idxend = s.indexOf(']', idxstart+1)
        iterate(s.drop(idxend+1), 
          SRMultiArrayType( typeCode,  s.slice(idxstart+1, idxend).toInt +: srtype.dims))
      }
      else 
        srtype
    }

    //  format name is varname[dim][dim]/T
    if (str.indexOf('[') == -1) typeCode // not an array type
    else iterate(str, SRMultiArrayType(typeCode, Seq()))  // check for array dims
  }

  /**
   * @return - Return the full Simple SR Data Type for this terminal branch
   */
  def assignType(branch: TBranch): SRType = 
  {
    val leavesTypes = branch.getTitle.split(":")
    if (leavesTypes.length>1) SRStructType(
      for (x <- leavesTypes) yield (
        if (x.contains('[')) x.slice(0, x.indexOf('['))
        else x.dropRight(2)
        , checkLeafForArrayType(x))
    )
    else
      checkLeafForArrayType(leavesTypes(0))
  }

  /**
   * @return - returns the AbstractSchemaTree RootNode
   */
  def buildAST(tree: TTree, streamers: Seq[TStreamerInfo]): AbstractSchemaTree = 
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
        val mytype = assignType(branch)
        val leaves = branch.getLeaves
        if (leaves.size==1)
        {
          val leaf = leaves.get(0).asInstanceOf[TLeaf]
          new TerminalNode(new Leaf(new LeafInfo(
            leaf.getName, leaf.getRootClass.getClassName, leaf.getLen
            )), new NodeInfo(
              branch.getName, branch.getTitle, branch.getRootClass.getClassName, mytype
            ), mytype.getIterator(branch))
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

    def synthesizeBranchElement(branchElement: TBranchElement): AbstractSchemaTree = 
    {
      val subs = branchElement.getBranches
      if (subs.size>0)
      {
        //  complex node element
        null
      }
      else
      {
        // ssimple node element
        null
      }
    }

    def synthesize(branch: TBranch): AbstractSchemaTree = 
    {
      if (branch.isInstanceOf[TBranchElement])
        synthesizeBranchElement(branch.asInstanceOf[TBranchElement])
      else
        synthesizeBranch(branch)
    }

    new RootNode(tree.getName,
      for (i <- 0 until tree.getNBranches; b=tree.getBranch(i))
        yield synthesize(b)
    )
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
      case NodeElement(subnodes, info) => StructField(info.name, StructType(
        for (x <- subnodes) yield iterate(x)
      ))
      case TerminalNodeElement(leaf, info, iter) => null
      case _ => null
    }
    
    ast match {
      case RootNode(_, nodes) => StructType(
        for (x <- nodes) yield iterate(x)
      ) 
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
      case TerminalMultiLeafNode(_, _, iter) => Row.fromSeq(iter.next)
      case NodeElement(subnodes, info) => Row.fromSeq(
        for (x <- subnodes) yield iterate(x)
      )
      case TerminalNodeElement(leaf, info, iter) => iter.next
      case _ => null
    }
    
    ast match {
      case RootNode(_, nodes) => Row.fromSeq(
        for (x <- nodes) yield iterate(x)
      )
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
      case Node(subnodes, info) => {
        println(("  "*level) + info)
        for (x <- subnodes) __print__(x, level+2)
      }
      case TerminalNode(leaf, info, iter) => 
        println(("  "*level) + info + " -> " + leaf.info)
      case TerminalMultiLeafNode(leaves, info, iter) => {
        print(("  "*level) + info + " -> " + leaves.map(_.info.toString).mkString(" :: "))
      }
      case NodeElement(subnodes, info) => {
        println(("  "*level) + info)
        for (x <- subnodes) __print__(x, level+2)
      }
      case TerminalNodeElement(leaf, info, iter) => 
        println(("  "*level) + info + " -> " + leaf.info)
      case _ => println(null)
    }

    __print__(ast, 0)
  }

  def containsNext(ast: AbstractSchemaTree): Boolean = ast match {
    case RootNode(name, nodes) => containsNext(nodes.head)
      case Node(subnodes, info) => containsNext(subnodes head)
      case TerminalNode(leaf, info, iter) => iter.hasNext
      case TerminalMultiLeafNode(_, _, iter) => iter.hasNext
      case NodeElement(subnodes, info) => containsNext(subnodes head)
      case TerminalNodeElement(leaf, info, iter) => false
      case _ => false
  }

  /**
   * some utils
   */
  def findTree(dir: TDirectory): TTree =
  {
    for (i <- 0 until dir.nKeys) {
      val obj = dir.getKey(i).getObject.asInstanceOf[core.AbstractRootObject]
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
}
