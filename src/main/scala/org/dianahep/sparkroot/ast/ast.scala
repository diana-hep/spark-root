package org.dianahep.sparkroot

import org.dianahep.sparkroot.ast._
import org.dianahep.root4j.interfaces._
import org.dianahep.root4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.util

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

  /*
  def assignBranchElementType(branch: TBranchElement, 
    streamerInfo: TStreamerInfo): SRType = 
  {

  }*/
  
  def assignLeafType(leaf: TLeaf): SRType = 
  {
    if (leaf.getArrayDim>0) // array
      SRArrayType(assignLeafTypeByLeafClass(leaf), leaf.getArrayDim)
    else
      assignLeafTypeByLeafClass(leaf)
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


  def arrangeStreamers(reader: RootFileReader): Map[String, TStreamerInfo] = 
  {
    val streamers = reader.streamerInfo
    (for (i <- 0 until streamers.size; s=streamers.get(i) 
      if s.isInstanceOf[TStreamerInfo]; streamer=s.asInstanceOf[TStreamerInfo])
      yield (streamer.getName, streamer)
    ).toMap
  }
}
