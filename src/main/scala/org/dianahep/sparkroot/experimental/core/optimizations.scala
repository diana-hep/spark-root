package org.dianahep.sparkroot.experimental.core

import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

package object optimizations {
  trait OptimizationPass {
    def run(x: SRRoot): SRRoot
  }

  case object RemoveNullTypePass extends OptimizationPass {
    private def iterate(t: SRType): SRType = t
    
    def run(root: SRRoot): SRRoot = 
      // assume there are no top level nulls
      SRRoot(root.name, root.entries, root.types.map(iterate(_)))
  }

  case object RemoveEmptyRowPass extends OptimizationPass {
    private def iterate(t: SRType): SRType = t match {
      case x: SRComposite => 
        if (x.split) 
          // if split => filter out the empty rows among the members and 
          // iterate through the rest of the members!
          SRComposite(x.name, x.b, 
            x.members.filterNot(checkIfEmptyComposite(_)).map(iterate(_)), 
            x.split, x.isTop, x.isBase, x._shouldDrop)
        else 
          // if not split => mark the empty rows (members) for dropping
          SRComposite(x.name, x.b,
            x.members.map {case m => if (checkIfEmptyComposite(m)) m.drop else m}, 
            x.split, x.isTop, x.isBase, x._shouldDrop)
        case x: SRVector => 
          // assume for now that t is not an empty composite!
          // because if it is => we shuold filter out the whole vector!
          SRVector(x.name, x.b, iterate(x.t),
            x.split, x.isTop, x._shouldDrop)
        case x: SRMap =>
          // same assumptions as for core.SRVector
          SRMap(x.name, x.b, iterate(x.keyType), iterate(x.valueType),
            x.split, x.isTop, x._shouldDrop)
        case x: SRMultiMap  =>
          SRMultiMap(x.name, x.b, iterate(x.keyType),
            iterate(x.valueType), x.split, x.isTop, x._shouldDrop)
        case x: SRType => x
    }

    private def checkIfEmptyComposite(t: SRType) = t match {
      case x: SRComposite => x.members.size==0
      case _ => false
    }

    def run(root: SRRoot): SRRoot = {
      SRRoot(root.name, root.entries, 
        // empty Rows at the top column level are not removed!
        root.types.map(iterate(_)))
    }
  }

  case object FlattenOutBasePass extends OptimizationPass {
    // all of the members of the base should go into newMembers
    // If a member is a base itself => descend further
    private def descend(newMembers: ListBuffer[SRType], 
        base: SRComposite): Unit = {
      println("descending")
      for (member <- base.members) member match {
        // member is a base composite
        case y @ SRComposite(_, _, _, _, _, true, _) => {
          println("double descend")
          descend(newMembers, y)
        }
        // member is not a base composite
        case _ => newMembers += iterate(member)
      }
    }
    
    private def iterate(t: SRType): SRType = t match {
      // assume that a type t is not a BASE Composite type!
      case x: SRArray => SRArray(x.name, x.b, x.l, iterate(x.t), x.n, x._shouldDrop)
      case x: SRSimpleType => x
      case x: SRVector => SRVector(x.name, x.b, iterate(x.t), x.split, 
        x.isTop, x._shouldDrop)
      case x: SRMap => SRMap(x.name, x.b, iterate(x.keyType), iterate(x.valueType),
        x.split, x.isTop, x._shouldDrop)
      case x: SRMultiMap => SRMultiMap(x.name, x.b, iterate(x.keyType),
        iterate(x.valueType), x.split, x.isTop, x._shouldDrop)
      case x: SRComposite => {
        // build a new composite with flattened out members for Base Composite members
        val newMembers = ListBuffer.empty[SRType]
        for (member <- x.members) member match {
          case SRComposite(_, _, _, _, _, true, _) => {
            println("base")
            descend(newMembers, member.asInstanceOf[SRComposite])
          }
          case _ => {println("not base");newMembers += iterate(member)}
        }
        println(s"newMembers for ${x.name}:")
        for (x <- newMembers) println(s"x = $x")
        SRComposite(x.name, x.b, newMembers, x.split, x.isTop, x.isBase, x._shouldDrop)
      }
      // unknowns/nulls/strings...
      case x: SRType => x
    }

    def run(root: SRRoot): SRRoot = {
      // there must be at least some non-empty top level columns
      println("running FlattenOutBasePass")
      SRRoot(root.name, root.entries, root.types.map(iterate(_)))
    }
  }

  case class PruningPass(requiredSchema: StructType) extends OptimizationPass {
    private def iterate(
        main: SRType,
        optRequiredType: Option[DataType]): SRType = main match {
      case x: SRSimpleType => optRequiredType match {
            // if type is not provided, this guy shuold be marked for dropping
            case None => x.drop
            // if the type is provided - we just leave as is
            case Some(tpe) => x
          }
      case x: SRCollection => optRequiredType match {
        case None => x.drop
        case Some(tpe) => x match {
          // for the array type check => iterate thru the  children
          case xx: SRVector => SRVector(xx.name, xx.b,
            iterate(xx.t, Some(tpe.asInstanceOf[ArrayType].elementType)),
            xx.split, xx.isTop)
              // for the rest just assign x. Map should come in full or String...
          case _ => x
        }
      }
      case x: SRNull => optRequiredType match {
        case None => x.drop
        case Some(tpe) => x
      }
      case x: SRUnknown => optRequiredType match {
        case None => x.drop
        case Some(tpe) => x
      }
      case x: SRComposite =>
        if (x.split) optRequiredType match {
          case None => // should not happen!
            x.drop
          case Some(tpe) =>
            // composite is split and is in the required schema
            if (x.members.size == 0) x
            else SRComposite(x.name, x.b,
              // tpe must be StructType
              tpe.asInstanceOf[StructType].fields.map({
                case field => iterate(x.members.find(
                  _.toName==field.name) match {
                    case Some(a) => a
                    case None =>
                      throw new Exception(s"An empty Composite being searched" + 
                        s" x.members.size==${x.members.size} \n${tpe.asInstanceOf[StructType].treeString} \n${printATT(x)}")}
                ,
                Some(field.dataType))}), x.split, x.isTop, x.isBase)
        } else optRequiredType match {
          case None =>
            // this composite should be read in but dropped
            x.drop
          case Some(tpe) =>
            // this composite is not splittable
            if (x.members.size == 0) x
            else SRComposite(x.name, x.b,
              x.members.map {case m => iterate(m,
                tpe.asInstanceOf[StructType].fields.find
                  {case field => field.name == m.toName}.map(_.dataType)
              )},
              x.split, x.isTop, x.isBase)
        }
      case x: SRType => optRequiredType match {
        case None => x.drop
        case Some(tpe) => x
      }
    }

    def run(root: SRRoot) = {
      SRRoot(root.name, root.entries,
        root.types zip requiredSchema.fields.map(_.dataType) map {
          case (left, right) => iterate(left, Some(right))
        })
    }
  }

  val basicPasses: Seq[OptimizationPass] = (Nil :+ RemoveEmptyRowPass 
    :+ FlattenOutBasePass) //\
//    :+ FlattenOutBasePass
}
