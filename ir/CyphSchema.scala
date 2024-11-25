package uk.ac.ed.dal
package raqlet
package ir

case class CyphSchema(types: Seq[ElementType])


sealed trait ElementType

case class NodeType(name: String, spec: LabelSpec, fields: Seq[(String, CyphType)], datainput: String) extends ElementType

case class EdgeType(src: LabelInput, dest: LabelInput, rel: LabelInput, fields: Seq[(String, CyphType)], datainput: String) extends ElementType

sealed trait LabelSpec

case class LabelInput(name: String) extends LabelSpec

case class LabelVar(name: String) extends LabelSpec

case class LabelOr(list: Seq[LabelInput]) extends LabelSpec

sealed trait CyphType

case object CLongIntType extends CyphType

case object CIntType extends CyphType

case object CStringType extends CyphType

case object UnknownType extends CyphType

object CyphSchema {
  def translate(cs: CyphSchema): DatalogSchema = {
    DatalogSchema(cs.types.flatMap(et => translateElem(et)(cs)).toList)
  }

  def translateElem(et: ElementType)(cs: CyphSchema): List[Either[Rule, Input]] = et match {
    case NodeType(name, spec, fields, fname) => spec match {
      case LabelInput(name) => {
        val decl = Decl(name, fields.map(arg => Arg(arg._1, arg._2)).toList)
        val input = Input(name, "file", fname)
        List(Left(decl), Right(input))
      }
      case LabelOr(listOfLabels) => {
        val decl = Decl(name, fields.map(arg => Arg(arg._1, arg._2)).toList)
        val listOfAttributes = fields.map(arg => arg._1)
        val defHead = Relation(name, decl.args.map(arg => arg.x))
        val rules = listOfLabels.map(label => {
          // get field of node type label
          val fields = cs.types.collectFirst{
            case NodeType(name, _, fields, _) if (name == label.name) => fields
          } match {
            case Some(fields) => fields
            case None => throw new Exception("ERROR: Faulty Schema! Components of Or type Node is not defined!")
          }

          val args = fields.map(pair => if (listOfAttributes.contains(pair._1)) {
            pair._1
          } else {
            "_"
          } ).toList
          Def(defHead, List(Relation(label.name, args)))
        }).toList
        List(Left(decl)) ++ rules.map(r => Left(r))
      }
      case LabelVar(_) => throw new Exception("Schema translation for LabelVar is not supported")
    }
    case EdgeType(src, dest, rel, fields, fname) => {
      if (rel.name.contains("undir")){
        val undirIDBName = src.name + "_" + rel.name + "_" + dest.name
        val sourceIDB = undirIDBName.replaceAll("_undir_", "_")
        val srcIdType = getNodeIdType(src)(cs)
        val destIdType = getNodeIdType(dest)(cs)
        val sourceDecl = Decl(sourceIDB, List(Arg("id1", srcIdType), Arg("id2", destIdType)) ++ fields.map(arg => Arg(arg._1, arg._2)))
        val sourceInput = Input(sourceIDB, "file", fname)
        val undirDecl = Decl(undirIDBName, List(Arg("id1", srcIdType), Arg("id2", destIdType)) ++ fields.map(arg => Arg(arg._1, arg._2)))
        val argsInBody = undirDecl.args.map(arg => arg.x)
        val argsInBody2 = argsInBody match {
          case head1 :: head2 :: tail => head2 :: head1 :: tail
        }
        val rules = List(Def(Relation(undirIDBName, argsInBody), List(Relation(sourceIDB, argsInBody))),
          Def(Relation(undirIDBName, argsInBody), List(Relation(sourceIDB, argsInBody2))))
        List(Left(sourceDecl), Right(sourceInput), Left(undirDecl)) ++ rules.map(r => Left(r))
      } else {
        val idbName = src.name + "_" + rel.name + "_" + dest.name
        // get node id type
        val srcIdType = getNodeIdType(src)(cs)
        val destIdType = getNodeIdType(dest)(cs)
        val decl = Decl(idbName, List(Arg("id1", srcIdType), Arg("id2", destIdType)) ++ fields.map(arg => Arg(arg._1, arg._2)))
        val input = Input(idbName, "file", fname)
        List(Left(decl), Right(input))
      }
    }
  }

  def getNodeIdType(nodeLabel: LabelInput)(schema: CyphSchema): CyphType = {
    schema.types.collectFirst{
      case NodeType(_, spec, fields, _) if (spec == nodeLabel) => fields.toMap.get("id")
    } match {
      case Some(Some(ty)) => ty
      case _ => throw new Exception(s"Type of node id for $nodeLabel is not found in schema")
    }
  }
}