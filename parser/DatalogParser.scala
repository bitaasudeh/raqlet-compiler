package uk.ac.ed.dal
package raqlet
package parser

import ir._
import fastparse.SingleLineWhitespace._
import fastparse.CharPredicates._
import fastparse._

object DatalogParser {

  def parse(inputString: String): (DatalogSchema, DatalogQuery) = {
    val parsedInput = removeEOF(inputString)
    val inputSchema: String = sortSchema(parsedInput)
    val inputQuery: String = sortQuery(parsedInput)

    (fastparse.parse(inputSchema, parseSchema(_)), fastparse.parse(inputQuery, parseQuery(_) )) match {
      case (Parsed.Success(schema, _), Parsed.Success(query, _)) => (schema, query)
      case (f1, f2) => 
        val fails = (f1, f2) match {
          case (_: Parsed.Failure, _: Parsed.Failure) => List(f1, f2)
          case (_, _: Parsed.Failure) => List(f2)
          case (_: Parsed.Failure, _) => List(f1)
          case (_, _) => List()
        }
        throw new Exception(s"Parse failed for dl`$inputString`:\n ${fails.mkString("\n******\n")}")
    }
  }

  def keywords[_: P] = P (
    StringIn("sum", "min", "count", ".input", ".output", ".decl",
      "number", "unsigned", "int", "symbol") ~
      !idRest
  )  

  def removeEOF(input: String) = {
    val lines = input.split("\n").filterNot(_.contains(".output")).mkString("\n")
    lines
  }

  def sortSchema(input: String): String = {
    val lines = input.split("\n").filterNot(_.contains(":-")).mkString("\n")
    lines
  }

  def sortQuery(input: String): String = {
    val lines = input.split("\n").filter(_.contains(":-")).mkString("\n")
    lines
  }

  def parseSchema[_: P]: P[DatalogSchema] = P(((parseLeftSchema | parseRightSchema) ~ newline.?).rep(1)).map {
    schemaList => DatalogSchema(schemaList.toList)
  }

  def parseLeftSchema[_: P]: P[Either[Rule, Input]] = P(parseRule.map(Left(_)))

  def parseRightSchema[_: P]: P[Either[Rule, Input]] = P(parseInput.map(Right(_)))

  def parseInput[_: P]: P[Input] = P(".input" ~ name ~ "(" ~ "IO=" ~ name ~ "," ~ "filename=" ~ """"""" ~ input ~ """"""" ~ ")").map {
    case (head, io, fname) => Input(head, io, fname)
  }

  def parseQuery[_: P]: P[DatalogQuery] = P((parseRule ~ newline.?).rep(1)).map {
    case (rules) => DatalogQuery(rules.toList)
  }

  def parseRule[_: P]: P[Rule] = P(parseDecl | parseDef)

  def parseDef[_: P]: P[Def] = P(parseRelation ~ ":-" ~ parseAtoms ~ ".".?).map {
    case (relation, atoms) => Def(relation, atoms)
  }

  def parseAtoms[_: P]: P[List[Atom]] = parseAtom.rep(1, sep = ",").map(atoms => atoms.toList)

  def parseAtom[_: P]: P[Atom] = P(parseDisjunction  | parseNegation | parseCmp | parseRelation | parens)

  def parens[_: P]: P[Atom] = P( "(" ~/ parseAtom ~/ ")") 

  // def parseCmp[_: P]: P[CmpRel] = P(
  //   name ~ op ~ (
  //     number.map(Right(_)) |
  //       (
  //         &(StringIn("sum ", "min ", "count ", "(")) ~/
  //           (
  //             parseAtom.map(Left(_)) |
  //               parseCmpInner.map(Left(_)) |
  //               parseDLVar.map(Left(_)) |
  //               name.map(Left(_))
  //             )
  //         ) |
  //       parseDLVar.map(Left(_)) |
  //       name.map(Left(_))
  //     )
  // ).map {
  //   case (left, op, Right(num)) => CmpRel(op, left, num)
  //   case (left, op, Left(atomOrName)) => CmpRel(op, left, atomOrName)
  // }

  def parseTerm[_: P]: P[Term] = P(parseMin | parseCount | parseSum | variable | parseArith | constant)
  def parseArith[_: P]: P[Arith] = P("(" ~ parseTerm ~ arithop ~ parseTerm ~ ")").map {
    case (t1, op, t2) => Arith(op, t1, t2)
  }

  def parseCmp[_: P]: P[CmpRel] = P(
    variable ~ cmpop ~ parseTerm
  ).map {
    case (left, op, right) => CmpRel(op, left, right)
  }

  def parseCmpInner[_: P]: P[CmpRel] = P("(" ~ parseCmp ~ ")")
  def parseDisjunction[_: P]: P[Disjunction] = P("(" ~ parseAtoms ~ ";" ~ parseAtoms ~ ")").map{
    case (e1, e2) => Disjunction(e1, e2)
  }


  def parseNegation[_: P]: P[Neg] = P("!" ~ parseRelation).map {
    case (relation) => Neg(relation)
  }

  def parseRelation[_: P]: P[Relation] = P(name ~ "(" ~ (name ~ (",").?).rep(1) ~ ")").map {
    case (name, strings) => Relation(name, strings.toList)
  }

  def parseCount[_: P]: P[Count] = P("count" ~ ":" ~ "{" ~ (parseAtom ~ (",").?).rep(1) ~ "}").map {
    case (atoms) => Count(atoms.toList)
  }

  def parseMin[_: P]: P[Min] = P("min" ~ name ~ ":" ~ "{" ~ parseAtom ~ "}").map {
    case (name, atom) => Min(name, atom)
  }

  def parseSum[_: P]: P[Sum] = P("sum" ~ name ~ ":" ~ "{" ~ parseAtom ~ "}").map {
    case (name, atom) => Sum(name, atom)
  }

  def parseDecl[_: P]: P[Decl] = P(".decl" ~ name ~ "(" ~ parseArg.rep(1) ~ ")").map {
    case (name, args) => Decl(name, args.toList)
  }

  def parseArg[_: P]: P[Arg] = P(name ~ ":" ~ parseType ~ (",").?).map {
    case (name, t) => Arg(name, t)
  }

  def parseType[_: P]: P[CyphType] = P(name).map {
    case ("number") => CLongIntType
    case ("unsigned") => CLongIntType
    case ("int") => CIntType
    case ("symbol") => CStringType
    case ("???") => UnknownType
  }

  def op[_: P]: P[String] = P(
    StringIn(">=", "<=", "!=") | CharIn("+-/*=≠><")
  ).!

  def cmpop[_: P]: P[String] = P(
    StringIn(">=", "<=", "!=") | CharIn("=≠><")
  ).!

  def arithop[_: P]: P[String] = P(
    CharIn("+-/*")
  ).!

  def newline[_: P]: P[Unit] = P("\n" | "\r\n" | "\r").rep(1)



  def input[_: P] = P(CharIn("""a-zA-Z0-9_.?""").rep(1).!)

  def name[_: P]: P[String] = P(CharIn("""a-zA-Z0-9"_?""").rep(1).!).map {
    case str if str.startsWith("\"") && str.endsWith("\"") => str.substring(1, str.length - 1)
    case str => str
  }

  def stringChars(c: Char) = c != '\"' && c != '\\'
  def strChars[_: P] = P( CharsWhile(stringChars) )
  def hexDigit[_: P]      = P( CharIn("0-9a-fA-F") )
  def unicodeEscape[_: P] = P( "u" ~ hexDigit ~ hexDigit ~ hexDigit ~ hexDigit )
  def escape[_: P]        = P( "\\" ~ (CharIn("\"/\\\\bfnrt") | unicodeEscape) )
  def alpha[_: P]         = P( CharPred(isLetter) )
  def stringLiteral[_: P] =
    P( "\"" ~/ (strChars | escape).rep.! ~ "\"")

  // def parseDLVar[_:P]: P[DlVar] = P(CharIn("""a-zA-Z0-9_?""").rep(1).!).map{
  //   case (str) => DlVar(str)
  // }

  def idRest[_: P]: P[Char] = P( CharPred(c => isLetter(c) | isDigit(c) | c == '_').! ).map(_(0))
  def variable[_: P]: P[DlVar] = P( !keywords ~ ((alpha | "_" | "$") ~ idRest.rep).!).map(x => DlVar(x))

  def number[_: P]: P[Long] = P(CharIn("\\-0-9_?").rep(1).!).map{
    case str => str.toLong
  }

  def constant[_: P]: P[Constant] = P(number | stringLiteral).map(Constant)


}