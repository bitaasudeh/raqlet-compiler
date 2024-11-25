package uk.ac.ed.dal
package raqlet
package ir

/* IR design decision

Global Environment: Rule name -> alias,
i.e. MATCH1 -> V1 as this info will be used for later
and schema. first field name, second field name, ...
Local Environment (only applies to each rule):

Given an original Datalog IR
------------|
.decl A(..) |
A(..):-     |
A(..):-     |
------------|  --> this will become one IR that has two "rules"
.decl B(...)

* */

case class SqlogProg(rules: List[SqlogElement]) // used for constructing WITH R AS ( ... )

sealed abstract class SqlogElement
case class RecursiveBlock(base: SqlogElement, recur_case: SqlogElement, head: SqlogHead) extends SqlogElement // deal with recursive case
case class RuleBlock(rule: SqlogRule) extends SqlogElement
case class RuleUnionBlock(rules: List[SqlogRule]) extends SqlogElement // will corresponds to SQL UNION of statement

case class SqlogRule(head: SqlogHead, body: SqlogBody)

case class SqlogHead(alias: SqlogVar, eas: List[SqlogExprAs])

case class SqlogBody(atoms: List[SqlogAtom])

case class SqlogExprAs(expr: SqlogExpr, alias: String)

sealed abstract class SqlogExpr

case class SqlogProperty(table: SqlogVar, field: SqlogVar) extends SqlogExpr

case class SqlogIntValue(value: Int) extends SqlogExpr

case class SqlogLongIntValue(value: Long) extends SqlogExpr

case class SqlogStringValue(value: String) extends SqlogExpr

case class SqlogVar(name: String) extends SqlogExpr

case class SqlogCoalesce(expr: SqlogExpr, default: SqlogExpr) extends SqlogExpr

sealed abstract class SqlogAtom extends SqlogExpr

case class RelationAccess(table: SqlogTable, attributes: List[SqlogProperty]) extends SqlogAtom

case class NegationAccess(table: SqlogTable, join: List[JoinCondition]) extends SqlogAtom

case class JoinCondition(elem1: SqlogExpr, elem2: SqlogExpr) extends SqlogAtom

case class BinaryOp(op: String, elem1: SqlogExpr, elem2: SqlogExpr) extends SqlogAtom

case object NoopOp extends SqlogAtom

// to be deprecated
case class SqlogSum(sumVar: SqlogExprAs, selectAndGroupVars: List[SqlogExprAs], joinFilterConditions: List[SqlogAtom], table: SqlogTable, view: SqlogVar) extends SqlogAtom
case class SqlogMin(minVar: SqlogExprAs, selectAndGroupVars: List[SqlogExprAs], table: SqlogTable, viewAlias: SqlogVar) extends SqlogAtom
case class SqlogCount(coalesceCount: SqlogProperty, countAlias: SqlogVar, groupByVars: List[SqlogExprAs], leftJoinCondition: List[SqlogAtom],
                      whereCondition: List[SqlogAtom], finalViewName: SqlogVar, table_left_name: SqlogTable, table_right_name: ViewTable) extends SqlogAtom

case object NoneAtom extends SqlogAtom

sealed abstract class SqlogTable

case class ViewTable(view: SqlogVar) extends SqlogTable

case class TableAlias(tblname: SqlogVar, alias: SqlogVar) extends SqlogTable

/* Environment
* tableAlias is for storing all original_relation tables -> alias, this will be used throughout the
* Datalog IR rewriting process;
* schema will be datalog schema, but really only decl is needed here.
* also newly created view table (V1, V2...) with its field, will be added to schema (tho type is not really needed)
* */

case class SqlogEnv(tableAlias: Map[SqlogVar, SqlogVar],
                    varOccur: Map[SqlogVar, List[SqlogProperty]],
                    unboundVars: List[SqlogVar],
                    inlineValues: Map[SqlogVar, SqlogExpr],
                    schema: DatalogSchema)








