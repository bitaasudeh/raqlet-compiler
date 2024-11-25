package uk.ac.ed.dal
package raqlet
package ir

/** SQL IR Design Decision Notes:
 * formalisation see
 * https://docs.google.com/presentation/d/1rEHAddollXJD0xvW0YRH3HKyP4NtxlGVj6fy47U7k3U/edit#slide=id.g2aa1a291e39_0_0
 * */

case class SQLQuery(withClause: SQLWith , stmt: SQLStatement)
case class SQLWith(cte: List[SQLCTE])

sealed abstract class SQLCTE
case class varAsStmt(alias: SQLVar, stmt: SQLStatement) extends SQLCTE
case class recurRelationAsStmt(rel: SQLRelation, stmt: SQLStatement) extends SQLCTE

case class SQLRelation(name: SQLVar, attributes: List[SQLVar])
sealed abstract class SQLStatement
case class SQLUnionStmt(stmt1: SQLStatement, stmt2: SQLStatement) extends SQLStatement // need to recursively construct UNION ...
case class SQLSingleStmt(distinct: Boolean, select: List[SQLSelectExpr], from: List[SQLTableExpr], where: List[SQLExpr], groupBy: List[SQLTerm]) extends SQLStatement

/* Select expression */
sealed abstract class SQLSelectExpr
case object SelectAll extends SQLSelectExpr
case class SelectConstValue(term: SQLTerm) extends SQLSelectExpr
case class SQLTermAsVar(term: SQLTerm, alias: SQLVar) extends SQLSelectExpr

/* From expression */
sealed abstract class SQLTableExpr
case class SQLStmtAsVar(stmt: SQLStatement, alias: SQLVar) extends SQLTableExpr
case class SQLTable(table: SQLVar) extends SQLTableExpr
case class SQLTableAsVar(table: SQLVar, alias: SQLVar) extends SQLTableExpr
case class SQLJoinedTable(join_type: SQLJoinType, table_right: SQLTableExpr, join_qual: List[SQLExpr]) extends SQLTableExpr

/* join type */
sealed abstract class SQLJoinType
case object SQLLeft extends SQLJoinType

/* Where expression */
sealed abstract class SQLExpr
case class SQLOr(expr1: SQLExpr, expr2: SQLExpr) extends SQLExpr
case class SQLAnd(expr1: SQLExpr, expr2: SQLExpr) extends SQLExpr
case class SQLBinaryOperation(op: String, term1: SQLTerm, term2: SQLTerm) extends SQLExpr
case class SQLNotExist(stmt: SQLStatement) extends SQLExpr

// basic elements
sealed abstract class SQLTerm
case class SQLVar(name: String) extends SQLTerm
case class SQLInt(value: Int) extends SQLTerm
case class SQLLongInt(value: Long) extends SQLTerm
case class SQLString(value: String) extends SQLTerm
case class SQLColumnRef(table: SQLVar, col: SQLVar) extends SQLTerm // i.e. R1.name
case class SQLSum(term: SQLTerm) extends SQLTerm
case class SQLMin(term: SQLTerm) extends SQLTerm
case class SQLCount(term: SQLTerm) extends SQLTerm
case class SQLArith(op: String, term1: SQLTerm, term2: SQLTerm) extends SQLTerm
case class SQLCoalesce(term: SQLTerm, default: SQLTerm) extends SQLTerm
case object SQLAll extends SQLTerm // i.e. *




