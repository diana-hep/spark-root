package org.dianahep.sparkroot

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.dianahep.root4j._
import org.dianahep.root4j.interfaces._
import scala.reflect.runtime.universe._

package object ast
{
    /**
     *  Abstract class for a simple iterator
     */
    trait hook;
    abstract class AbstractBranchIterator[T: TypeTag](branch: TBranch) 
      extends Iterator[Array[T]] with hook
    {
      //  leaf - currently just have 1 leaf per branch - full splitting
      private val leaf = branch.getLeaves.get(0).asInstanceOf[TLeaf]
      //  array of entry numbers that are first in each basket
      private val startingEntries = branch.getBasketEntry
      //  get the # bytes for a given leaf type
      private val typeSize = leaf.getLenType()

      private var basket = 0
      private var entry = 0L
      private def lastInBranch = entry == startingEntries(basket + 1) - 1
      def makeOutput(size: Int): Array[T]
      def readOne(rootInput: core.RootInput): T
      def hasNext = basket != startingEntries.size - 1

      def next() = {
        val endPosition =
          if (lastInBranch) {
            // the endPosition comes from a byte marker in the ROOT header
            val rootInput = branch.setPosition(leaf, entry)
            basket += 1 // this is the last entry in the basket, better update the basket number
            rootInput.getLast
          } else {
            // the endPosition is where the next entry starts (in this basket)
            val rootInput = branch.setPosition(leaf, entry + 1)
            rootInput.getPosition
          }

        // actually get the data
        val rootInput = branch.setPosition(leaf, entry)
        // create an array with the right size
        val out = makeOutput((endPosition - rootInput.getPosition).toInt / typeSize)
        // fill it (while loops are faster than any Scalarific construct)
        var i = 0
        while (rootInput.getPosition < endPosition) {
          out(i) = readOne(rootInput)
          i += 1
        }
        // update the entry number and return the array
        entry += 1L
        out
      }
    }

    case class BranchIteratorFloat(branch: TBranch) 
      extends AbstractBranchIterator[Float](branch)
    {
      override def makeOutput(size: Int) = Array.fill[Float](size)(0.0f)
      override def readOne(rootInput: core.RootInput) = rootInput.readFloat
    }
    case class BranchIteratorInteger(branch: TBranch) 
      extends AbstractBranchIterator[Int](branch)
    {
      override def makeOutput(size: Int) = Array.fill[Int](size)(0)
      override def readOne(rootInput: core.RootInput) = rootInput.readInt
    }
    case class BranchIteratorDouble(branch: TBranch) 
      extends AbstractBranchIterator[Double](branch)
    {
      override def makeOutput(size: Int) = Array.fill[Double](size)(0)
      override def readOne(rootInput: core.RootInput) = rootInput.readDouble
    }
    case class BranchIteratorShort(branch: TBranch) 
      extends AbstractBranchIterator[Short](branch)
    {
      override def makeOutput(size: Int) = Array.fill[Short](size)(0)
      override def readOne(rootInput: core.RootInput) = rootInput.readShort
    }
    case class BranchIteratorString(branch: TBranch)
      extends AbstractBranchIterator[String](branch)
    {
      override def makeOutput(size: Int) = Array.fill[String](size)("")
      override def readOne(rootInput: core.RootInput) = rootInput.readString
    }
    case class BranchIteratorLong(branch: TBranch) 
      extends AbstractBranchIterator[Long](branch)
    {
      override def makeOutput(size: Int) = Array.fill[Long](size)(0)
      override def readOne(rootInput: core.RootInput) = rootInput.readLong
    }
    case class BranchIteratorByte(branch: TBranch) 
      extends AbstractBranchIterator[Byte](branch)
    {
      override def makeOutput(size: Int) = Array.fill[Byte](size)(0)
      override def readOne(rootInput: core.RootInput) = rootInput.readByte
    }

    class LeafInfo(
        val name: String, 
        val title: String, 
        val classname: String, 
        val len: Int, // #fixed size elements
        val ltype: Int)  // leaf type
    {
        override def toString = (name, title, classname, len, ltype).toString
    }
    class NodeInfo(
      val name: String, 
      val title: String, 
      val classname: String, 
      val iter: hook,
      val streamer_type: Int, // Type of the Streamer
      val branch_type: Int, // Type of the Branch - Type of the TBranchElement
      val objClassName: String)
    {
        override def toString = (name, title, classname,
          streamer_type, branch_type, objClassName).toString
    }
    abstract class AbstractSchemaTree;
    case class Leaf(info: LeafInfo) extends AbstractSchemaTree
    case class ComplexNode(subnodes: Seq[AbstractSchemaTree], info: NodeInfo) extends AbstractSchemaTree;
    case class SimpleNode(leaf: AbstractSchemaTree, info: NodeInfo) extends AbstractSchemaTree;


    def typeByLeafClassName(className: String): DataType = className match {
      case "TLeaf" => null
      case "TLeafB" => ByteType
      case "TLeafC" => StringType
      case "TLeafD" => DoubleType
      case "TLeafF" => FloatType
      case "TLeafI" => IntegerType
      case "TLeafL" => LongType
      case "TLeafS" => ShortType
      case _ => null
    }                                       
    def iteratorByLeafClassName(branch: TBranch, className: String): hook = 
      className match {
        case "TLeaf" => null
        case "TLeafB" => new BranchIteratorByte(branch)
        case "TLeafC" => new BranchIteratorString(branch)
        case "TLeafD" => new BranchIteratorDouble(branch)
        case "TLeafF" => new BranchIteratorFloat(branch)
        case "TLeafI" => new BranchIteratorInteger(branch)
        case "TLeafL" => new BranchIteratorLong(branch)
        case "TLeafS" => new BranchIteratorShort(branch)
        case _ => null
    }                               
    def getNext(className:String, iter: hook) = className match
    {
        case "TLeaf" => null
        case "TLeafB" => iter.asInstanceOf[BranchIteratorByte].next
        case "TLeafC" => iter.asInstanceOf[BranchIteratorString].next
        case "TLeafD" => iter.asInstanceOf[BranchIteratorDouble].next
        case "TLeafF" => iter.asInstanceOf[BranchIteratorFloat].next
        case "TLeafI" => iter.asInstanceOf[BranchIteratorInteger].next
        case "TLeafL" => iter.asInstanceOf[BranchIteratorLong].next
        case "TLeafS" => iter.asInstanceOf[BranchIteratorShort].next
        case _ => null
    }
    def getHasNext(className:String, iter: hook) = className match
    {
        case "TLeaf" => false
        case "TLeafB" => iter.asInstanceOf[BranchIteratorByte].hasNext
        case "TLeafC" => iter.asInstanceOf[BranchIteratorString].hasNext
        case "TLeafD" => iter.asInstanceOf[BranchIteratorDouble].hasNext
        case "TLeafF" => iter.asInstanceOf[BranchIteratorFloat].hasNext
        case "TLeafI" => iter.asInstanceOf[BranchIteratorInteger].hasNext
        case "TLeafL" => iter.asInstanceOf[BranchIteratorLong].hasNext
        case "TLeafS" => iter.asInstanceOf[BranchIteratorShort].hasNext
        case _ => false
    }

    def typeByLeaf(leaf: AbstractSchemaTree): DataType = leaf match
    {
      case Leaf(info) =>
        if (info.len>1) ArrayType(typeByLeafClassName(info.classname))
        else typeByLeafClassName(info.classname)
      case _ => null
    }

    def generateSparkSchema(ast: AbstractSchemaTree): StructType =
    {
        def iterate(node: AbstractSchemaTree): StructField = node match
        {
            case ComplexNode(subnodes, info) => StructField(info.name, StructType(
                for (x <- subnodes) yield iterate(x)
            ))
            case SimpleNode(leaf, info) => StructField(info.name, typeByLeaf(leaf))
        }

        ast match {
            case ComplexNode(subnodes, info) => StructType(
                for (x <- subnodes) yield iterate(x)
            )
            case _ => null
        }
    }

    def containsNext(ast: AbstractSchemaTree): Boolean = 
    {
      def iterate(node: AbstractSchemaTree): Boolean = node match
      {
        case ComplexNode(subnodes, _) => iterate(subnodes(0))
        case SimpleNode(leaf, info) => getHasNext(leaf match {
          case Leaf(linfo) => linfo.classname}, info.iter)
      }
      
      ast match 
      {
        case ComplexNode(subnodes, _) => iterate(subnodes(0))
        case _ => false
      }
    }

    def generateSparkRow(ast: AbstractSchemaTree): Row =
    {
        def iterate(node: AbstractSchemaTree): Any = node match
        {
            case ComplexNode(subnodes, info) => Row.fromSeq(
                for (x <- subnodes) yield iterate(x)
            )
            case SimpleNode(leaf, info) => 
              leaf match {
                case Leaf(linfo) => {
                  val data = getNext(linfo.classname, info.iter)
                  if (linfo.len==1) data.head 
                  else data
                }
              }
        }

        ast match {
            case ComplexNode(subnodes, info) => Row.fromSeq(
                for (x <- subnodes) yield iterate(x)
            )
            case _ => null
        }
    }

    def buildAST(tree: TTree): AbstractSchemaTree =
    {
        def iterate(branch: TBranch): AbstractSchemaTree =
        {
            if (branch.getBranches.size()>0)
            {
              if (branch.isInstanceOf[TBranchElement])
              {
                val branchElement = branch.asInstanceOf[TBranchElement]
                new ComplexNode(
                    {
                      for (i <- 0 until branchElement.getBranches.size; 
                      b=branchElement.getBranches.get(i).asInstanceOf[TBranch]) 
                      yield iterate(b)
                    },
                    new NodeInfo(branchElement.getName, 
                      branchElement.getTitle, 
                      branchElement.getRootClass.getClassName, null,
                      branchElement.getStreamerType, branchElement.getType,
                      branchElement.getClassName))
              }
              else 
                new ComplexNode(
                {for (i <- 0 until branch.getBranches.size; 
                  b=branch.getBranches.get(i).asInstanceOf[TBranch]) yield iterate(b)},
                  new NodeInfo(branch.getName, branch.getTitle, 
                    branch.getRootClass.getClassName, null, -1, -1, ""
                ))
            } // if a complex node or a simple node
            else
            {
              val l = branch.getLeaves.get(0).asInstanceOf[TLeaf]
              if (l.isInstanceOf[TLeafElement])
              {
                val ll = l.asInstanceOf[TLeafElement]
                val branchElement = branch.asInstanceOf[TBranchElement]
                new SimpleNode(new Leaf(new LeafInfo(
                  ll.getName, ll.getTitle, ll.getRootClass.getClassName, 
                  ll.getLen, ll.getType
                )), new NodeInfo(branchElement.getName, branchElement.getTitle, 
                  branchElement.getRootClass.getClassName, null, 
                  branchElement.getStreamerType, branchElement.getType, 
                  branchElement.getClassName))
              }
              else
                new SimpleNode(new Leaf(new LeafInfo(
                    l.getName, l.getTitle, l.getRootClass.getClassName, l.getLen, -1
                )), new NodeInfo(branch.getName, branch.getTitle, 
                  branch.getRootClass.getClassName,
                  iteratorByLeafClassName(branch, l.getRootClass.getClassName),
                  -1, -1, ""))
            }
        }

        new ComplexNode(
            for (i <- 0 until tree.getBranches.size; 
              b=tree.getBranch(i).asInstanceOf[TBranch]) yield
              iterate(b), new NodeInfo(tree.getName, tree.getTitle, "TTree", 
                null,-1,-1,""))
    }

    def printAST(ast: AbstractSchemaTree): Unit =
    {
      def __print__(node: AbstractSchemaTree, level: Int): Unit = node match {
        case ComplexNode(subnodes, info) =>
          {
            println(("  " * level) + info)
            for (b <- subnodes) __print__(b, level + 2)
          }
        case SimpleNode(leaf, info) => leaf match {
            case Leaf(linfo) => print(("  " * level) + info + " -> " + linfo + "\n")
        }
      }

      __print__(ast, 0)
    }

    def findTree(dir: TDirectory): TTree =
    {
      for (i <- 0 until dir.nKeys) {
        val obj = dir.getKey(i).getObject.asInstanceOf[core.AbstractRootObject]
        if (obj.getRootClass.getClassName == "TDirectory" ||
          obj.getRootClass.getClassName == "TTree") {
          if (obj.getRootClass.getClassName == "TDirectory")
            return findTree(obj.asInstanceOf[TDirectory])
          else (obj.getRootClass.getClassName == "TTree")
          return obj.asInstanceOf[TTree]
        }
      }
      null
    }
}
