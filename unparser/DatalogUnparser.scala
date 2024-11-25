package uk.ac.ed.dal
package raqlet
package unparser

import ir._

object DatalogUnparser {
  def unparseSchema(sc: DatalogSchema): String = {
    sc.schema.map {
      case Left(rule) => unparseRule(rule)
      case Right(input) => unparseInput(input)
    }.mkString("\n")
  }
  def unparse(d: DatalogQuery): String = d.rules.map(unparseRule).mkString("\n") + "\n" + ".output Return"

  def unparseInput(i: Input): String = {
    s""".input ${i.head}(IO=${i.io}, filename="${i.fname}")"""
  }

  def unparseRule(r: Rule): String = r match {
    case Decl(a, args) => s".decl $a(${args.map(arg => s"${arg.x}: ${unparseType(arg.t)}").mkString(", ")})"
    case Def(a, b) => s"${unparseAtom(a)} :- ${b.map(unparseAtom).mkString(", ")}."
  }

  def unparseTerm(term: Term): String = term match {
    case Sum(arg, body) => s"sum $arg : { ${unparseAtom(body)} }"
    case Min(arg, body) => s"min $arg : { ${unparseAtom(body)} }"
    case Count(atomList) => s"count : { ${atomList.map(unparseAtom).mkString(", ")} }"
    case DlVar(v) => v
    case Arith(op, t1, t2) => s"(${unparseTerm(t1)} $op ${unparseTerm(t2)})"
    case Constant(s: String) => s""""${s}""""
    case Constant(v) => v.toString
  }

  def unparseAtom(atom: Atom): String = atom match {
    case Relation(a, x) => s"${a}(${x.mkString(", ")})"
    // case CmpRel(op, x1, x2) => {
    //   x2 match {
    //     case s: String => s"""$x1 ${op} "${s}""""
    //     case v: DlVar => s"$x1 ${op} ${v.name}"
    //     case v: Long => s"$x1 ${op} $v"
    //     case v: Int => s"$x1 ${op} $v"
    //     case c: CmpRel => s"$x1 ${op} (${unparseAtom(c.asInstanceOf[Atom])}) "
    //     case _ => s"$x1 ${op} ${unparseAtom(x2.asInstanceOf[Atom])}"
    //   }
    // }
    case CmpRel(op, t1, t2) => s"${unparseTerm(t1)} $op ${unparseTerm(t2)}"
    case Neg(rel) => s"!${unparseAtom(rel)}"
    case Disjunction(expr1, expr2) => s"( ${expr1.map(unparseAtom).mkString(", ")} ; ${expr2.map(unparseAtom).mkString(", ")} )"
  }

  def unparseType(t: CyphType): String = t match {
    case CLongIntType => "number"
    case CIntType => "number"
    case CStringType => "symbol"
    case UnknownType => "???"
  }
}