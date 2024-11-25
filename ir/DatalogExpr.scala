package uk.ac.ed.dal
package raqlet
package ir

case class DatalogQuery(rules: List[Rule])

sealed abstract class Rule

case class Decl(a: String, args: List[Arg]) extends Rule

case class Def(a: Relation, b: List[Atom]) extends Rule

sealed abstract class Atom

case class Relation(a: String, x: List[String]) extends Atom

case class CmpRel(op: String, x1: Term, x2: Term) extends Atom

case class Neg(a: Relation) extends Atom

case class Disjunction(expr1: List[Atom], expr2: List[Atom]) extends Atom // todo: really this should be a list of conjunctions
case class Arg(x: String, t: CyphType)

sealed trait Term

case class DlVar(name: String) extends Term
case class Constant(v: Any) extends Term
case class Arith(op: String, t1: Term, t2: Term) extends Term
case class Sum(arg: String, atom: Atom) extends Term
case class Min(arg: String, atom: Atom) extends Term
case class Count(args: List[Atom]) extends Term


// Datalog Schema

case class Input(head: String, io: String, fname: String)

case class DatalogSchema(schema: List[Either[Rule, Input]])



