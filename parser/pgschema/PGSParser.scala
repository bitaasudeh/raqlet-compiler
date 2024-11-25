package uk.ac.ed.dal.raqlet.parser.pgschema

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.JavaTokenParsers
import java.io.{BufferedReader, FileReader, IOException}
import scala.io.Source
import java.io._
import scala.util.matching.Regex
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import uk.ac.ed.dal.raqlet.ir._
import fastparse.NoWhitespace._
import fastparse.CharPredicates._
import fastparse._

// Parse PG-Schema using Fastparse Library.
// Each production rule in grammar (defined in PG-Scheam paper) corresponds to a method in the class.
object PGSchemaParser {
  var isOr: Boolean = false

  def graphType[_: P]: P[CyphSchema] = P(
    "CREATE" ~ ws ~ "GRAPH" ~ ws ~ "TYPE" ~ ws ~ typeName ~ ws ~ graphTypeMode ~ ws ~ graphTypeImports.? ~ ws.? ~ "{" ~ ws.? ~ elementTypes.? ~ ws.? ~ "}"
  ).map { case (tName, Some(gTypeElement)) =>
    CyphSchema(gTypeElement)
  }

  def graphTypeMode[_: P]: P[Unit] = P(("STRICT" | "LOOSE").?)

  def graphTypeImports[_: P]: P[Unit] = P(
    "IMPORTS" ~ ws ~ typeName ~ ws.? ~ ("," ~ ws.? ~ typeName).rep
  )

  def elementTypes[_: P]: P[Seq[ElementType]] =
    P(elementType.rep(min = 1, sep = sep)).map { case (first) =>
      first.toList
    }

  def elementType[_: P]: P[ElementType] = P(nodeType | edgeType)

  def nodeType[_: P]: P[NodeType] =
    P(
      "(" ~ ws.? ~ typeName ~ ws.? ~ (":" ~ ws.? ~ labelOr).? ~ ws.? ~ "{" ~ ws.? ~ properties ~ ws.? ~ "}" ~ ws.? ~ ")"
    )
      .map { case (tname, Some(lor), prop) =>
        var tnameModified =
          tname.replaceAll("Type", "").replaceAll("\"", "").capitalize
        if (isOr)
          NodeType(tnameModified, lor, prop, "")
        else
          NodeType(tnameModified, lor, prop, s"${tnameModified}.facts")
      }

  def edgeType[_: P]: P[EdgeType] = P(
    "(" ~ ws.? ~ ":" ~ ws.? ~ label ~ ws.? ~ ")" ~ ws.? ~ "-" ~ ws.? ~ "[" ~ ws.? ~ typeName ~ ws.? ~
      ":" ~ ws.? ~ label ~ ws.? ~ "{" ~ ws.? ~ properties ~ ws.? ~ "}" ~ ws.? ~ "]" ~ ws.? ~ "->" ~ ws.? ~ "(" ~ ws.? ~ ":" ~ ws.? ~ label ~ ws.? ~ ")"
  ).map { case (tnamesrc, _, lname, propSpec, tnamedest) =>
    val ninput = s"${tnamesrc.name}_${lname.name}_${tnamedest.name}.facts"
      .replaceAll("\"", "")
      .toLowerCase
    EdgeType(tnamesrc, tnamedest, lname, propSpec, ninput)
  }

  def labelOr[_: P]: P[LabelSpec] =
    P(label ~ ws.? ~ (ws.? ~ "|" ~ ws.? ~ label).rep).map {
      case (first, rest) if rest.isEmpty => {
        isOr = false
        first // first is a LabelInput which LabelInput extends LabelSpec
      }
      case (first, rest) => {
        isOr = true
        LabelOr(first :: rest.toList) // Return LabelOr if rest is not empty
      }
    }

  def properties[_: P]: P[Seq[(String, CyphType)]] =
    P(property.rep(min = 1, sep = sep)).map { case (prop) =>
      prop.toList
    }

  def property[_: P]: P[(String, CyphType)] =
    P("OPTIONAL".? ~ ws ~ key ~ ws ~ propertyType).map {
      case (keyName, propType) =>
        keyName -> propType
    }

  def typeName[_: P]: P[String] =
    P((CharIn("a-zA-Z") ~ CharIn("a-zA-Z0-9_").rep).!).map { case (tname) =>
      tname
    }

  def label[_: P]: P[LabelInput] =
    P((CharIn("a-zA-Z") ~ CharIn("a-zA-Z0-9_").rep).!).map { case (labelName) =>
      LabelInput(labelName)
    }

  def key[_: P]: P[String] =
    P((CharIn("a-zA-Z") ~ CharIn("a-zA-Z0-9_").rep).!).map { case (nkey) =>
      nkey
    }

  def ws[_: P]: P[Unit] = P(
    CharIn(" \n\t\r").rep
  )

  def sep[_: P]: P[Unit] = P(ws.? ~ "," ~ ws.?)

  def propertyType[_: P]: P[CyphType] =
    P(StringIn("CLongIntType", "CStringType", "CIntType").!).map {
      case ("CLongIntType") => CLongIntType
      case ("CStringType")  => CStringType
      case ("CIntType")     => CIntType
    }

  def parseInput(input: String): CyphSchema = {
    // Use FastParse to parse the input string
    fastparse.parse(input, graphType(_)) match {
      case Parsed.Success(value, _) =>
        value
      case Parsed.Failure(label, index, extra) =>
        throw new IllegalArgumentException(
          s"Parsing failed at index $index: expected $label but found ${input.drop(index)}"
        )
    }
  }
}
