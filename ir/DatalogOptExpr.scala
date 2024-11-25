package uk.ac.ed.dal
package raqlet
package ir


/** Datalog Intermediate Representation used for query optimisation
 * Designed as a wrapper on top of the Datalog IR, idea is to make it easier
 * for implementing transpiler optimisation functionalities.
 * */

object relationTypeEnum extends Enumeration {
  type relationTypeEnum = Value
  val singleIDB, unionIDB, nontcIDB, tcIDB, singleNodeEDB, singleEdgeEDB, unionNodeEDB, unionEdgeEDB = Value
}


case class datalogQueryOptIR(rules: List[idbOptIR])
case class RelationInfo(rel_type: relationTypeEnum.Value, size: Int, decl: Decl, definition: Option[List[Def]])
case class optimiserEnv(relationTable: Map[String, RelationInfo], fully_inlined_table: Set[String],
                        dont_inline: Set[String], query_depth: Map[String, Int]) // need decl for EDB

sealed abstract class idbOptIR

case class singleNormDefOptIR(a: Relation, b: List[Atom]) extends idbOptIR
case class unionNormDefOptIR(branches: List[singleNormDefOptIR]) extends idbOptIR
case class ntClosureDefOptIR(branches: List[singleNormDefOptIR]) extends idbOptIR
case class tClosureDefOptIR(branches: List[singleNormDefOptIR]) extends idbOptIR
case object NonDefOptIR extends idbOptIR
