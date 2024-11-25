package uk.ac.ed.dal
package raqlet
package unparser

import ir._

object CyphUnparser {
  def unparse(c: CypherQuery): String = c match {
    case Return(attrs) => s"RETURN DISTINCT ${attrs.map(unparseAS).mkString(", ")}"
    case ClauseQuery(clause, query) => s"${unparseClause(clause)}${unparse(query)}"
  }

  def unparseAS(exprAs: ExprAs): String = s"${unparseExpr(exprAs.e)} AS ${exprAs.a.strg}"

  def unparsePattern(pattern: Pattern): String = pattern match {
      case NodePattern(vrb, label) => s"MATCH (${vrb.strg}:$label)\n"
      case EdgePattern(src, trg, vrb, label, dist, direct) => direct match {
        case "direct" => s"MATCH (${src.vrb.strg}:${src.label})-[${vrb.strg}:$label${unparseDist(dist)}]->(${trg.vrb.strg}:${trg.label})\n"
        case "undirect" => s"MATCH (${src.vrb.strg}:${src.label})-[${vrb.strg}:$label${unparseDist(dist)}]-(${trg.vrb.strg}:${trg.label})\n"
      }
    }

  def unparseClause(clause: Clause): String = clause match {
    case Match(pattern) => unparsePattern(pattern)

    case Where(exprs) => s"WHERE ${unparseExpr(exprs)}\n"
    case With(exprsAs) => s"WITH DISTINCT ${exprsAs.map(unparseAS).mkString(", ")}\n"
  }

  def unparseDist(dist: Option[Distance]): String = dist match {
    case Some(TransDistance(m)) => s"$m..."
    case Some(NonTransDistance(m1, m2)) => s"$m1...$m2"
    case None => ""
  }

  def unparseExpr(expr: Expr): String = expr match {
    case Value(v) => s"$v"
    case Var(a) => s"$a"
    case Key(vrb, key) => s"${vrb.strg}.${key.strg}"
    case Cmp(op, e1, e2) => s"${unparseExpr(e1)}$op${unparseExpr(e2)}"
    case Binop(op, e1, e2) => s"${unparseExpr(e1)} $op ${unparseExpr(e2)}"
    case Not(e) => s"NOT (${unparseExpr(e)})"
    case PatternExpr(pats, None) => pats.map(unparsePattern).mkString(", ")
  }

}