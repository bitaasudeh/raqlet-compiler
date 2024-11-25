package uk.ac.ed.dal
package raqlet

import ir._
import parser._
import unparser._
import transformer._
import parser.pgschema._

import java.io._
import scala.io.Source

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.JavaTokenParsers
import java.io.{BufferedReader, FileReader, IOException}
import scala.io.Source
import java.io._
import scala.util.matching.Regex
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Main {
  def main(args: Array[String]): Unit = {

    // Check if at least one command-line argument is provided
    if (args.length < 1) {
      println("Usage: sbt 'run <absolute_path_of_query_directory> <option>'")
      System.exit(1)
    }

    // The first argument is the absolute path of the directory
    val directoryPath = args(0)
    val targetOption = args(1)
    if (targetOption == "datalog" || targetOption == "sql"){
      // List files in the directory
      val fullpath = System.getProperty("user.home") + s"/${directoryPath}"
      val directory = new java.io.File(fullpath)
      if (directory.isDirectory) {
        val files = directory.listFiles()
        val schemaLDBC = CyphSchema(Seq(
          NodeType("Person", LabelInput("Person"), Seq(
            "id" -> CLongIntType, "firstName" -> CStringType, "lastName" -> CStringType, "gender" -> CStringType,
            "birthday" -> CLongIntType, "creationDate" -> CLongIntType, "locationIP" -> CStringType, "browserUsed" -> CStringType,
            "speaks" -> CStringType, "email" -> CStringType
          ), "Person.csv"),
          NodeType("City", LabelInput("City"), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), "City.csv"),
          NodeType("Country", LabelInput("Country"), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), "Country.csv"),
          NodeType("Continent", LabelInput("Continent"), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), "Continent.csv"),
          NodeType("Place", LabelOr(List(LabelInput("City"), LabelInput("Country"), LabelInput("Continent"))), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), ""),
          NodeType("Company", LabelInput("Company"), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), "Company.csv"),
          NodeType("University", LabelInput("University"), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), "University.csv"),
          NodeType("Organisation", LabelOr(List(LabelInput("University"), LabelInput("Company"))), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), ""),
          NodeType("Tag", LabelInput("Tag"), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), "Tag.csv"),
          NodeType("TagClass", LabelInput("TagClass"), Seq(
            "id" -> CLongIntType, "name" -> CStringType, "url" -> CStringType
          ), "TagClass.csv"),
          NodeType("Forum", LabelInput("Forum"), Seq(
            "id" -> CLongIntType, "title" -> CStringType, "creationDate" -> CLongIntType
          ), "Forum.csv"),
          NodeType("Message", LabelOr(List(LabelInput("Post"), LabelInput("Comment"))), Seq(
            "id" -> CLongIntType, "creationDate" -> CLongIntType, "locationIP" -> CStringType, "browserUsed" -> CStringType,
            "content" -> CStringType, "length" -> CLongIntType), ""),
          NodeType("Comment", LabelInput("Comment"), Seq(
            "id" -> CLongIntType, "creationDate" -> CLongIntType, "locationIP" -> CStringType, "browserUsed" -> CStringType,
            "content" -> CStringType, "length" -> CLongIntType), "Comment.csv"),
          NodeType("Post", LabelInput("Post"), Seq(
            "id" -> CLongIntType, "imageFile" -> CStringType, "creationDate" -> CLongIntType, "locationIP" -> CStringType, "browserUsed" -> CStringType,
            "language" -> CStringType, "content" -> CStringType, "length" -> CLongIntType), "Post.csv"),
          EdgeType(LabelInput("Post"), LabelInput("Tag"), LabelInput("HAS_TAG"), Seq("id" -> CLongIntType), "post_hasTag_tag.csv"),
          EdgeType(LabelInput("Comment"), LabelInput("Tag"), LabelInput("HAS_TAG"), Seq("id" -> CLongIntType), "comment_hasTag_tag.csv"),
          EdgeType(LabelInput("Company"), LabelInput("Country"), LabelInput("IS_LOCATED_IN"), Seq("id" -> CLongIntType), "company_isLocatedIn_country.csv"),
          EdgeType(LabelInput("University"), LabelInput("City"), LabelInput("IS_LOCATED_IN"), Seq("id" -> CLongIntType), "university_isLocatedIn_city.csv"),
          EdgeType(LabelInput("Person"), LabelInput("Company"), LabelInput("WORK_AT"), Seq("id" -> CLongIntType, "workFrom" -> CLongIntType), "person_workAt_organisation.csv"),
          EdgeType(LabelInput("Person"), LabelInput("University"), LabelInput("STUDY_AT"), Seq("id" -> CLongIntType, "classYear" -> CLongIntType), "person_studyAt_organisation.csv"),
          EdgeType(LabelInput("Person"), LabelInput("City"), LabelInput("IS_LOCATED_IN"), Seq("id" -> CLongIntType), "person_isLocatedIn_place.csv"),
          EdgeType(LabelInput("Person"), LabelInput("Person"), LabelInput("KNOWS_undir"), Seq("id" -> CLongIntType, "creationDate" -> CLongIntType), "person_knows_person.csv"),
          EdgeType(LabelInput("City"), LabelInput("Country"), LabelInput("IS_PART_OF"), Seq("id" -> CLongIntType), "city_isPartOf_country.csv"),
          EdgeType(LabelInput("Country"), LabelInput("Continent"), LabelInput("IS_PART_OF"), Seq("id" -> CLongIntType), "country_isPartOf_continent.csv"),
          EdgeType(LabelInput("Comment"), LabelInput("Person"), LabelInput("HAS_CREATOR"), Seq("id" -> CLongIntType), "comment_hasCreator_person.csv"),
          EdgeType(LabelInput("Post"), LabelInput("Person"), LabelInput("HAS_CREATOR"), Seq("id" -> CLongIntType), "post_hasCreator_person.csv"),
          EdgeType(LabelInput("Tag"), LabelInput("TagClass"), LabelInput("HAS_TYPE"), Seq("id" -> CLongIntType), "tag_hasType_tagclass.csv"),
          EdgeType(LabelInput("TagClass"), LabelInput("TagClass"), LabelInput("IS_SUBCLASS_OF"), Seq("id" -> CLongIntType), "tagclass_isSubclassOf_tagclass.csv"),
          EdgeType(LabelInput("Comment"), LabelInput("Comment"), LabelInput("REPLY_OF"), Seq("id" -> CLongIntType), "comment_replyOf_comment.csv"),
          EdgeType(LabelInput("Comment"), LabelInput("Post"), LabelInput("REPLY_OF"), Seq("id" -> CLongIntType), "comment_replyOf_post.csv"),
          EdgeType(LabelInput("Comment"), LabelInput("Country"), LabelInput("IS_LOCATED_IN"), Seq("id" -> CLongIntType), "comment_isLocatedIn_place.csv"),
          EdgeType(LabelInput("Post"), LabelInput("Country"), LabelInput("IS_LOCATED_IN"), Seq("id" -> CLongIntType), "post_isLocatedIn_place.csv"),
          EdgeType(LabelInput("Forum"), LabelInput("Post"), LabelInput("CONTAINER_OF"), Seq("id" -> CLongIntType), "forum_containerOf_post.csv"),
          EdgeType(LabelInput("Forum"), LabelInput("Tag"), LabelInput("HAS_TAG"), Seq("id" -> CLongIntType), "forum_hasTag_tag.csv"),
          EdgeType(LabelInput("Forum"), LabelInput("Person"), LabelInput("HAS_MODERATOR"), Seq("id" -> CLongIntType), "forum_hasModerator_person.csv"),
          EdgeType(LabelInput("Forum"), LabelInput("Person"), LabelInput("HAS_MEMBER"), Seq("id" -> CLongIntType, "joinDate" -> CLongIntType), "forum_hasMember_person.csv"),
          EdgeType(LabelInput("Person"), LabelInput("Post"), LabelInput("LIKES"), Seq("id" -> CLongIntType, "creationDate" -> CLongIntType), "person_likes_post.csv"),
          EdgeType(LabelInput("Person"), LabelInput("Comment"), LabelInput("LIKES"), Seq("id" -> CLongIntType, "creationDate" -> CLongIntType), "person_likes_comment.csv"),
          EdgeType(LabelInput("Person"), LabelInput("Tag"), LabelInput("HAS_INTEREST"), Seq("id" -> CLongIntType), "person_hasInterest_tag.csv"),
        ))

        // Read and translate each Cypher query
        files.foreach { file =>
          val filePath = s"${fullpath}/${file.getName}"
          val queryName = file.getName.stripSuffix(".cypher")
          // Debugging
          println(s"Translating Query ${queryName} now...")
          val cypherQueryInput = Source.fromFile(filePath).mkString
          val datalogSchema = CyphSchema.translate(schemaLDBC)
          val schemaTranslated = DatalogUnparser.unparseSchema(datalogSchema)
          val inputAST = CypherParser.parse(cypherQueryInput)
          val firstPassInputAst = CyphAstLower.lowerOpenCypherAst(inputAST)
          val secPassInputAst = CyphRewrite.rewriteCyphAst(firstPassInputAst)(schemaLDBC)
          val env = CypherEnv(CypherPrevRel(List(), None, List()), Map(), schemaLDBC, Map(), List(), Map(), List(), Map(), Map(), Map())
          val translatedAst = CypherToDatalog.translate(secPassInputAst)(env)
          // Datalog Optimisation
          val ast_rewrite = DatalogRewrite.rewrite(translatedAst)
          val ast_optimised = DatalogOptimiser.optimise(ast_rewrite)(datalogSchema)

          // write to file
          if (targetOption == "datalog") {
            val queryActual = DatalogUnparser.unparse(ast_optimised)
            val finalOutput = schemaTranslated + "\n" + queryActual

            val filename = System.getProperty("user.home") + s"/raqlet-bench/datalog-inline/${queryName}.dl"
            val queryfile = new File(filename)
            val bw = new BufferedWriter(new FileWriter(queryfile))
            bw.write(finalOutput)
            bw.close()
            println(s"Datalog Query ${queryName} has been translated and saved at ${filename}")
          } else if (targetOption == "sql") {
            // Datalog to SQL
            val sqlogenv = SqlogEnv(Map(), Map(), List(), Map(), datalogSchema)
            val sqlogQuery = SqlogRewrite.rewrite(ast_optimised)(sqlogenv)
            val sqlConstructs = SqlogLower.lowerToSQL(sqlogQuery)
            val finalSQLOutput = SqlUnparser.unparseQuery(sqlConstructs)

            val filename = System.getProperty("user.home") + s"/raqlet-bench/sql-queries-inline/${queryName}.sql"
            val queryfile = new File(filename)
            val bw = new BufferedWriter(new FileWriter(queryfile))
            bw.write(finalSQLOutput)
            bw.close()
            println(s"SQL Query ${queryName} has been translated and saved at ${filename}")

          }

        }

      } else {
        println(s"$fullpath is not a valid directory.")
        System.exit(1)
      }

    }
    // Check if target option is pgschema, parse all files in PG-Schema format and convert to desired form and save them in files.
    else if (targetOption == "pgschema") {

      val fullpath = System.getProperty("user.home") + s"/${directoryPath}"
      val directory = new java.io.File(fullpath)

      if (directory.isDirectory) {
        val files = directory.listFiles()
        files.foreach { file =>
          val filePath = s"${fullpath}/${file.getName}"
          val fileName = file.getName.stripSuffix(".txt")
          // Read from files.
          var PGQueryInput = Source.fromFile(filePath).mkString
          // Parse each file.
          PGQueryInput = PGQueryInput.trim
          var resultFunction = PGSchemaParser.parseInput(PGQueryInput)
          println("====================================")
          println(resultFunction)
          // Write the result string to a file
          val newFilename = System.getProperty("user.home") + s"/raqlet-bench/pgschema/${fileName}.txt"
          val newfile = new File(newFilename)
          val bw = new BufferedWriter(new FileWriter(newfile))
          bw.write(resultFunction.toString)  // Write the result string to the file
          bw.close()
          println(s"try to parse ${fileName} file, result has been saved at ${newFilename}")
          println("====================================")
        }
      }
      // Check if path is invalid exit from program. 
      else {
              println(s"$fullpath is not a valid directory.")
              System.exit(1)
            }

    }
  }
}