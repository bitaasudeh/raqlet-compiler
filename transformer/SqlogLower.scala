package uk.ac.ed.dal
package raqlet
package transformer

import ir._

object SqlogLower {
  def lowerToSQL(prog: SqlogProg): SQLQuery = {
    val cteStmts = prog.rules.map(lowerElement)
    val withFinal = SQLWith(cteStmts)
    val lastCTEName = cteStmts.last match {
      case varAsStmt(alias, _) => SQLTable(alias)
      case recurRelationAsStmt(relation, _) => SQLTable(relation.name)
    }
    val stmtFinal = SQLSingleStmt(true, List(SelectAll), List(lastCTEName), List(), List())
    SQLQuery(withFinal, stmtFinal)
  }

  def lowerElement(elem: SqlogElement): SQLCTE = elem match {
    case RuleBlock(rule) => {
      val stmt = lowerSqlogRule(rule)
      val viewAlias = SQLVar(rule.head.alias.name)
      varAsStmt(viewAlias, stmt)
    }
    case RuleUnionBlock(rules) => {
      val singleStmtList = rules.map(lowerSqlogRule)
      val finalStmt = singleStmtList.reduceLeft(SQLUnionStmt)
      val viewAlias = SQLVar(rules.head.head.alias.name)
      varAsStmt(viewAlias, finalStmt)
    }
    case RecursiveBlock(base, recur_case, head) => {
      val baseStmt = lowerElement(base) match {
        case varAsStmt(_, stmt) => stmt
        case _ => throw new Exception("nested recursion is not supported")
      }
      val recurStmt = lowerElement(recur_case) match {
        case varAsStmt(_, stmt) => stmt
        case _ => throw new Exception("nested recursion is not supported")
      }
      val unifiedStmt = SQLUnionStmt(baseStmt, recurStmt)
      val relationName = SQLVar(head.alias.name)
      val relAttrs = head.eas.map(eas => SQLVar(eas.alias))
      recurRelationAsStmt(SQLRelation(relationName, relAttrs), unifiedStmt)
    }
  }

  def lowerSqlogRule(rule: SqlogRule): SQLStatement = {
    val selectElem = rule.head.eas.map(lowerSqlogExprAs)
    val fromElem = rule.body.atoms.flatMap(lowerFromConstruct)
    val whereElem = rule.body.atoms.flatMap(lowerSqlogCondition)
    val whereExpr = whereElem match {
      case Nil => Nil
      case head :: Nil => whereElem
      case _ => List(whereElem.reduceLeft(SQLAnd))
    }
    SQLSingleStmt(true, selectElem, fromElem, whereExpr, List())
  }

  def lowerSqlogExprAs(eas: SqlogExprAs): SQLTermAsVar = eas.expr match {
    case SqlogProperty(table, field) => SQLTermAsVar(SQLColumnRef(SQLVar(table.name), SQLVar(field.name)), SQLVar(eas.alias))
    case SqlogIntValue(value) => SQLTermAsVar(SQLInt(value), SQLVar(eas.alias))
    case SqlogLongIntValue(value) => SQLTermAsVar(SQLLongInt(value), SQLVar(eas.alias))
    case SqlogStringValue(value) => SQLTermAsVar(SQLString(value), SQLVar(eas.alias))
    case BinaryOp(op, elem1, elem2) => SQLTermAsVar(SQLArith(op, lowerSqlogExpr(elem1), lowerSqlogExpr(elem2)), SQLVar(eas.alias))
    case SqlogCoalesce(elem1, elem2) => SQLTermAsVar(SQLCoalesce(lowerSqlogExpr(elem1), lowerSqlogExpr(elem2)), SQLVar(eas.alias))
  }

  // A bit duplicate with function lowerSqlogExprAs
  def lowerSqlogExpr(expr: SqlogExpr): SQLTerm = expr match {
    case SqlogProperty(table, col) => SQLColumnRef(SQLVar(table.name), SQLVar(col.name))
    case SqlogIntValue(v) => SQLInt(v)
    case SqlogLongIntValue(v) => SQLLongInt(v)
    case SqlogStringValue(v) => SQLString(v)
    case SqlogVar(name) => SQLVar(name)
    case BinaryOp(op, elem1, elem2) => SQLArith(op, lowerSqlogExpr(elem1), lowerSqlogExpr(elem2))
    case SqlogCoalesce(elem1, elem2) => SQLCoalesce(lowerSqlogExpr(elem1), lowerSqlogExpr(elem2))
  }

  /** Lower SqlogAtom that constructs FROM in SQL
   * this includes RelationAccess, SqlogSum
   * */
  def lowerFromConstruct(atom: SqlogAtom): Option[SQLTableExpr] = atom match {
    case RelationAccess(table, _) => Some(lowerSqlogTable(table))
    case s: SqlogSum => Some(lowerSumConstruct(s))
    case m: SqlogMin => Some(lowerMinConstruct(m))
    case c: SqlogCount => Some(lowerCountConstruct(c))
    case _ => None
  }

  def lowerSumConstruct(atom: SqlogSum): SQLTableExpr = {

    val sumTerm = SQLTermAsVar(SQLSum(lowerSqlogExpr(atom.sumVar.expr)), SQLVar(atom.sumVar.alias))
    val selectStmt = atom.selectAndGroupVars.map(lowerSqlogExprAs) :+ sumTerm
    val fromStmt = List(lowerSqlogTable(atom.table))
    val whereStmt = List()
    val groupStmt = atom.selectAndGroupVars.map(eas => lowerSqlogExpr(eas.expr))
    val stmt = SQLSingleStmt(distinct = false, selectStmt, fromStmt, whereStmt, groupStmt)
    val alias = SQLVar(atom.view.name)
    val join_on_qual = atom.joinFilterConditions.flatMap(lowerSqlogCondition)
    val joinOnStmt = join_on_qual match {
      case Nil => Nil
      case head :: Nil => join_on_qual
      case _ => List(join_on_qual.reduceLeft(SQLAnd))
    }
    SQLJoinedTable(SQLLeft, SQLStmtAsVar(stmt, alias), joinOnStmt)
  }

  def lowerMinConstruct(atom: SqlogMin): SQLTableExpr = {
    val minTerm = SQLTermAsVar(SQLMin(lowerSqlogExpr(atom.minVar.expr)), SQLVar(atom.minVar.alias))
    val selectStmt = atom.selectAndGroupVars.map(lowerSqlogExprAs) :+ minTerm
    val fromStmt = List(lowerSqlogTable(atom.table))
    val whereStmt = List()
    val groupStmt = atom.selectAndGroupVars.map(eas => lowerSqlogExpr(eas.expr))
    val stmt = SQLSingleStmt(distinct = false, selectStmt, fromStmt, whereStmt, groupStmt)
    val alias = SQLVar(atom.viewAlias.name)
    SQLStmtAsVar(stmt, alias)
  }

  // Lower SqlogCount to FROM (SELECT...COUNT(*) AS ...) AS R1
  def lowerCountConstruct(atom: SqlogCount): SQLTableExpr = {

    val coalesceCountTerm = SQLTermAsVar(SQLCoalesce(lowerSqlogExpr(atom.coalesceCount),SQLInt(0)), SQLVar(atom.countAlias.name))
    val parentSelectStmt = atom.groupByVars.map(lowerSqlogExprAs) :+ coalesceCountTerm

    val childSelectStmt = atom.groupByVars.map(lowerSqlogExprAs) :+ SQLTermAsVar(SQLCount(SQLAll), SQLVar(atom.countAlias.name))
    val childFromStmt = List(lowerSqlogTable(atom.table_left_name))
    val childWhereStmt = atom.whereCondition.flatMap(lowerSqlogCondition)
    val childWhereStmtExpr = childWhereStmt match {
      case Nil => Nil
      case head :: Nil => childWhereStmt
      case _ => List(childWhereStmt.reduceLeft(SQLAnd))
    }
    val childGroupByStmt = atom.groupByVars.map(eas => lowerSqlogExpr(eas.expr))

    val subqueryForLeftJoin = SQLSingleStmt(distinct = false, select = childSelectStmt, from = childFromStmt, where = childWhereStmtExpr, groupBy = childGroupByStmt)
    val subqueryAsVar = SQLStmtAsVar(subqueryForLeftJoin, SQLVar(atom.table_right_name.view.name))

    val leftJoinCond = atom.leftJoinCondition.flatMap(lowerSqlogCondition)
    val leftJoinCondExpr = leftJoinCond match {
      case Nil => Nil
      case head :: Nil => leftJoinCond
      case _ => List(leftJoinCond.reduceLeft(SQLAnd))
    }
    val leftJoinBaseTable = lowerSqlogTable(atom.table_left_name)
    val leftJoinTable = SQLJoinedTable(SQLLeft, subqueryAsVar, leftJoinCondExpr)
    val parentFromStmt = List(leftJoinBaseTable, leftJoinTable)

    val parentStmt = SQLSingleStmt(distinct = false, parentSelectStmt, parentFromStmt, List(), List())
    val subqueryAlias = SQLVar(atom.finalViewName.name)
    SQLStmtAsVar(parentStmt, subqueryAlias)
  }

  /** Lower SqlogAtom that constructs WHERE in SQL
   * this includes JoinCondtion and BinaryOp
   * */
  def lowerSqlogCondition(atom: SqlogAtom): Option[SQLExpr] = atom match {
    case JoinCondition(expr1, expr2) => Some(SQLBinaryOperation("=", lowerSqlogExpr(expr1), lowerSqlogExpr(expr2)))
    case BinaryOp(op, elem1, elem2) => Some(SQLBinaryOperation(op, lowerSqlogExpr(elem1), lowerSqlogExpr(elem2)))
    case NegationAccess(table, joins) => {
      val selectStmt = List(SelectConstValue(SQLInt(1)))
      val fromStmt = List(lowerSqlogTable(table))
      val whereStmt = joins.flatMap(lowerSqlogCondition)
      val whereExpr = whereStmt match {
        case Nil => Nil
        case head :: Nil => whereStmt
        case _ => List(whereStmt.reduceLeft(SQLAnd))
      }
      val stmt = SQLSingleStmt(true, selectStmt, fromStmt, whereExpr, List())
      Some(SQLNotExist(stmt))
    }
    case _ => None
  }

  def lowerSqlogTable(table: SqlogTable): SQLTableExpr = table match {
    case ViewTable(table) => SQLTable(SQLVar(table.name))
    case TableAlias(table, alias) => SQLTableAsVar(SQLVar(table.name), SQLVar(alias.name))
  }
}