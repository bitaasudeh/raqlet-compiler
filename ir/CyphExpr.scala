package uk.ac.ed.dal
package raqlet
package ir

sealed abstract class CypherQuery

case class Return(attrs: List[ExprAs]) extends CypherQuery

case class ClauseQuery(c: Clause, q: CypherQuery) extends CypherQuery


sealed abstract class Clause

case class Match(p: Pattern) extends Clause

case class UnionMatch(p: List[Pattern]) extends Clause

case class OptionalMatch(matches: List[Clause]) extends Clause

case class Where(expr: Expr) extends Clause

case class With(items: List[ExprAs]) extends Clause

case class WithDistinct(items: List[ExprAs]) extends Clause

case class WithCollect(expr: Expr, attribute: Var, relation: Var, groupKey: List[Var], unwind: Boolean) extends Clause

case class ShortestPath(pattern: PatternExpr, pathVar: Var) extends Clause


sealed abstract class Expr

case class Value(v: Any) extends Expr

case class Var(strg: String) extends Expr

case class Case(expr: Option[Expr], whenExpr: Expr, thenExpr: Expr, elseExpr: Expr) extends Expr

case class Key(a: Var, k: Var) extends Expr

case class Cmp(op: String, e1: Expr, e2: Expr) extends Expr

case class Binop(op: String, e1: Expr, e2: Expr) extends Expr

case class In(expr: Expr, relation: Var, neg: Boolean) extends Expr

case class AggSum(arg: Expr, groupKeys: List[Var]) extends Expr

case class AggCount(arg: Expr, groupKeys: List[Var]) extends Expr

case class AggSize(arg: Expr) extends Expr

case class AggMin(arg: Expr) extends Expr

case class Distinct(expr: Expr) extends Expr // for now

case class ListComp(variable: Var, table: Var, predicate: Expr, groupKeys: List[Var]) extends Expr

case class Not(expr: Expr) extends Expr

case class IsNULL(expr: Expr) extends Expr

case class PatternExpr(patterns: List[Pattern], property: Option[Expr]) extends Expr

case class LengthPath(path: Either[Var, Clause]) extends Expr

sealed abstract class Pattern

case class NodePattern(vrb: Var, label: String) extends Pattern

case class EdgePattern(src: NodePattern, trg: NodePattern, vrb: Var, label: String, dist: Option[Distance], direction: String) extends Pattern


sealed abstract class Distance

case class TransDistance(m: Int) extends Distance

case class NonTransDistance(m1: Int, m2: Int) extends Distance

case class ExprAs(e: Expr, a: Var)

// Schema
case class Schema(label: String, category: String, attr: Map[String, Int])

