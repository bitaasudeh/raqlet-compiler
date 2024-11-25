package uk.ac.ed.dal
package raqlet
package parser

import ir._

import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.parser.javacc.Cypher
import org.opencypher.v9_0.parser.javacc.CypherCharStream
import org.opencypher.v9_0.util.CypherExceptionFactory
import org.opencypher.v9_0.util.OpenCypherExceptionFactory
import org.opencypher.v9_0.util.InputPosition

import org.opencypher.v9_0.ast.factory.neo4j.{Neo4jASTFactory, Neo4jASTExceptionFactory}

case object CypherParser {

  /**
   * @param queryText The query to be parsed.
   * @param cypherExceptionFactory A factory for producing error messages related to the specific implementation of the language.
   * @return
   */
  def parse(
    queryText: String,
    cypherExceptionFactory: CypherExceptionFactory = OpenCypherExceptionFactory(None)
  ): Statement = {
    val charStream = new CypherCharStream(queryText)
    val astFactory = new Neo4jASTFactory(queryText)
    val astExceptionFactory = new Neo4jASTExceptionFactory(cypherExceptionFactory)

    val statements = new Cypher(astFactory, astExceptionFactory, charStream).Statements()
    if (statements.size() == 1) {
      statements.get(0)
    } else {
      throw cypherExceptionFactory.syntaxException(
        s"Expected exactly one statement per query but got: ${statements.size}",
        InputPosition.NONE
      )
    }
  }
}