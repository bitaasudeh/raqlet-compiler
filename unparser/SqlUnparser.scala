package uk.ac.ed.dal
package raqlet
package unparser

import ir._

object SqlUnparser {

  var recursionFlag = false

  def unparseQuery(query: SQLQuery): String = {
    recursionFlag = false
    val withStr = unparseWith(query.withClause)
    val stmtStr = unparseStmt(query.stmt)
    withStr + "\n" + stmtStr
  }

  def unparseWith(w: SQLWith): String = {
    val body = w.cte.map(unparseSQLCTE).mkString(", ")
    val recKeyWord = if (recursionFlag) {"RECURSIVE"} else {""}
    s"WITH ${recKeyWord} " + body
  }

  def unparseSQLCTE(cte: SQLCTE): String = cte match {
    case vas: varAsStmt => unparseVarAsStmt(vas)
    case rec: recurRelationAsStmt => unparseRecurRelationAsStmt(rec)
  }

  def unparseVarAsStmt(vas: varAsStmt): String = {
    val alias = unparseTerm(vas.alias)
    val body = unparseStmt(vas.stmt)
    alias + " AS (\n" + body + "\n)"
  }

  def unparseRecurRelationAsStmt(rec: recurRelationAsStmt): String = {
    if (!recursionFlag) {
      recursionFlag = true
    }
    s"${unparseTerm(rec.rel.name)}(${rec.rel.attributes.map(unparseTerm).mkString(",")}) AS (\n${unparseStmt(rec.stmt)}\n)"
  }

  def reorderFromStmt(stmt: List[SQLTableExpr]): List[SQLTableExpr] = {
    stmt.sortBy {
      case t: SQLJoinedTable => 1
      case _ => 0
    }
  }

  def unparseStmt(stmt: SQLStatement): String = stmt match {
    case SQLUnionStmt(stmt1, stmt2) => {
      "(\n" + unparseStmt(stmt1) + "\n)" + " UNION " + "(\n" + unparseStmt(stmt2) + "\n)"
    }
    case SQLSingleStmt(distinct, select, from, where, groupBy) => {
      val distinctSyntax = if (distinct) "DISTINCT" else ""
      val selectStr = s"SELECT ${distinctSyntax} " + select.map(unparseSelect).mkString(", ")
//      val fromStr = "\nFROM " + from.map(unparseFrom).mkString(", ")
      // no "," before left join
      val fromStr = "\nFROM " + reorderFromStmt(from).map(unparseFrom).zipWithIndex.map { case (item, index) =>
        if (index > 0 && !item.startsWith("\nLEFT")) {
          ", " + item
        } else {
          " " + item
        }
      }.mkString.trim

      val whereStr = where match {
        case Nil => ""
        case _ => "\nWHERE " + where.map(unparseSQLExpr).mkString(", ")
      }
      val groupByStr = groupBy match {
        case Nil => ""
        case _ => "\nGROUP BY " + groupBy.map(unparseTerm).mkString(", ")
      }
      selectStr + fromStr + whereStr + groupByStr
    }
  }

  def unparseSelect(expr: SQLSelectExpr): String = expr match {
    case SQLTermAsVar(term, alias) => {
      unparseTerm(term) + " AS " + unparseTerm(alias)
    }
    case SelectAll => "*"
    case SelectConstValue(term) => unparseTerm(term)
  }

  def unparseFrom(expr: SQLTableExpr): String = expr match {
    case SQLStmtAsVar(stmt, alias) => {
      val stmtStr = unparseStmt(stmt)
      val aliasStr = unparseTerm(alias)
      s"(\n${stmtStr}\n) AS ${aliasStr}"
    }
    case SQLTableAsVar(table, alias) => {
      val tableStr = unparseTerm(table)
      val aliasStr = unparseTerm(alias)
      s"${tableStr} AS ${aliasStr}"
    }
    case SQLTable(alias) => {
      s"${unparseTerm(alias)}"
    }
//    case SQLJoinedTable(table_left, join_type, table_right, join_qual) => {
//      s"${unparseFrom(table_left)} ${unparseJoinType(join_type)} ${unparseFrom(table_right)}\n" + "ON " + join_qual.map(unparseSQLExpr).mkString(", ")
//    }
    case SQLJoinedTable(join_type, table_right, join_qual) => {
      s"\n${unparseJoinType(join_type)} ${unparseFrom(table_right)}\nON ${join_qual.map(unparseSQLExpr).mkString(",")}"
    }
  }

  def unparseSQLExpr(expr: SQLExpr): String = expr match {
    case SQLOr(expr1, expr2) => {
      s"( ${unparseSQLExpr(expr1)} ) OR ( ${unparseSQLExpr(expr2)} )"
    }
    case SQLAnd(expr1, expr2) => {
      s"( ${unparseSQLExpr(expr1)} ) AND ( ${unparseSQLExpr(expr2)} )"
    }
    case SQLBinaryOperation(op, term1, term2) => {
      s" ${unparseTerm(term1)} ${op} ${unparseTerm(term2)} "
    }
    case SQLNotExist(stmt) => {
      s" NOT EXISTS ( \n ${unparseStmt(stmt)} \n )"
    }
  }

  def unparseTerm(expr: SQLTerm): String = expr match {
    case SQLAll => "*"
    case SQLVar(name) => name
    case SQLInt(v) => v.toString
    case SQLLongInt(v) => v.toString
    case SQLString(v) => s"'${v}'"
    case SQLColumnRef(table, col) => unparseTerm(table) + "." + unparseTerm(col)
    case SQLSum(term) => s"sum(${unparseTerm(term)})"
    case SQLMin(term) => s"min(${unparseTerm(term)})"
    case SQLCount(term) => s"count(${unparseTerm(term)})"
    case SQLArith(op, term1, term2) => s"(${unparseTerm(term1)} ${op} ${unparseTerm(term2)})"
    case SQLCoalesce(term, default) => s"COALESCE(${unparseTerm(term)}, ${unparseTerm(default)})"
  }

  def unparseJoinType(expr: SQLJoinType): String = expr match {
    case SQLLeft => "LEFT JOIN"
  }

}