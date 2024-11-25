package uk.ac.ed.dal
package raqlet
package transformer

import ir._

object DatalogRewrite {
  def rewrite(query: DatalogQuery): DatalogQuery = {
    // decompose disjunction into two rules
    val queryWithoutDisjunct = desugarDisjunction(query)
    val reorderedQuery = reorderRelation(queryWithoutDisjunct)
    val removedQuery = removeSameVarEqual(reorderedQuery)

    removedQuery
  }

  def desugarDisjunction(query: DatalogQuery): DatalogQuery = {
    val rewrittenRules: List[Rule] = query.rules.flatMap{
      case decl: Decl => List(decl)
      case idb: Def => {
        idb.b.collectFirst{ case d: Disjunction => d} match {
          case Some(disjunctAtom) => {
            val allExceptDisjunc = idb.b.filterNot(_.isInstanceOf[Disjunction])
            val atomList1 = disjunctAtom.expr1
            val atomList2 = disjunctAtom.expr2
            List(Def(idb.a, allExceptDisjunc ++ atomList1), Def(idb.a, allExceptDisjunc ++ atomList2))
          }
          case None => List(idb)
        }
      }
    }
    DatalogQuery(rewrittenRules)
  }
  def reorderRelation(query: DatalogQuery): DatalogQuery = {
    val reorderedRules = query.rules.map{
      case decl: Decl => decl
      case idb: Def => {
        val reoprderedBody = idb.b.sortBy{
          case _: Relation => 0
          case _: Neg => 2
          case _ => 1
        }
        Def(idb.a,reoprderedBody)
      }
    }
    DatalogQuery(reorderedRules)
  }

def removeSameVarEqual(query: DatalogQuery): DatalogQuery = {
  val reducedRules = query.rules.map {
    case decl: Decl => decl
    case idb: Def => _removeSameVarEqual(idb)
  }
  DatalogQuery(reducedRules)
}

def _removeSameVarEqual(rule: Def): Def = {
    val updatedBody = rule.b.flatMap {
      case cmpRel@CmpRel("=", left, right) =>
        (left, right) match {
          case (DlVar(v1), DlVar(v2)) if v1 == v2 => None
          case _ => Some(cmpRel)
        }
      case other => Some(other)
    }
    Def(rule.a, updatedBody)
  }
}