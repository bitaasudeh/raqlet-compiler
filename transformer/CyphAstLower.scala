package uk.ac.ed.dal
package raqlet
package transformer

import ir._

import org.opencypher.v9_0.{ast, expressions, util}

case class LowerEnv(binding: Map[Var, Clause], curGroupBy: List[Var]) // groupBy only reflects the current Clause

object CyphAstLower {

  def lowerOpenCypherAst(queryAst: ast.Statement): CypherQuery = {
    val clauseList = lowerQueryAst(queryAst)
    val returnClause = clauseList.last match {
      case Right(clause) => clause
      case _ => throw new Exception("clauseList does not have a Return clause in the end")
    }
    val queryClauses = clauseList.dropRight(1).map {
      case Left(clause) => clause
      case _ => throw new Exception("clauseList has non-Clause elements")
    }
    queryClauses.foldRight[CypherQuery](returnClause)((cur, acc) => ClauseQuery(cur, acc))
  }

  private def lowerQueryAst(queryAst: ast.Statement): Seq[Either[Clause, CypherQuery]] = queryAst match {
    case ast.Query(_, ast.SingleQuery(clauses)) => {
      val returnClause = clauses.last
      val clauseBeforeReturn = clauses.dropRight(1)
      val initEnv = LowerEnv(Map(), List())
      val (loweredClause, finalEnv) = clauseBeforeReturn.foldLeft[(List[Clause], LowerEnv)]((List(), initEnv))((acc, cur) => cur match {
        case ast.Unwind(expr, variable) => (lowerUnwindAst(expr, variable)(acc._1), acc._2) // in-place update
        case _ => {
          val (newloweredClause, nenv) = lowerClauseAst(cur)(acc._2)
          (acc._1 ++ newloweredClause, nenv)
        }
      })
      val loweredReturn = lowerReturnClauseAst(returnClause)(finalEnv)
      loweredClause.map(Left(_)) ++ loweredReturn.map(Right(_))
    }
    case unknown => throw new Exception(s"$unknown is not supported in Query AST lowering")
  }

  private def lowerClauseAst(clause: ast.Clause)(env: LowerEnv): (List[Clause], LowerEnv) = clause match {
    case ast.Match(option, pattern, _, where) => lowerMatchAst(pattern)(where)(option)(env)
    case ast.With(false, returnItems, _, _, _, where) => lowerWithAst(returnItems)(where)(env)
    case ast.With(true, returnItems, _, _, _, where) => lowerWithDistinctAst(returnItems)(where)(env)
    case unknown => throw new Exception(s"$unknown is not supported in Clause AST lowering")
  }

  private def lowerReturnClauseAst(clause: ast.Clause)(env: LowerEnv): List[CypherQuery] = clause match {
    case ast.Return(_, returnItems, _, _, _, _) => {
      // Get groupKeys if there are any aggregation in the Return clause
      val nenv = if (existAggregator(returnItems)) {
        val groupKeys = getGroupKeysInReturn(returnItems)
        // update groupkeys in Env
        LowerEnv(env.binding, groupKeys)
      } else {
        env
      }
      lowerReturnAst(returnItems)(nenv)
    }
    case _ => throw new Exception("Non-Return clause is called by lowerReturnClauseAst")
  }

  private def lowerReturnAst(returnItems: ast.ReturnItems)(env: LowerEnv): List[CypherQuery] = returnItems match {
    case ast.ReturnItems(_, items) => List(Return(items.map(lowerReturnItemAst(_)(env)).toList.filterNot(_ == null))) // null comes from collect
    case unknown => throw new Exception(s"$unknown is not supported in Return AST lowering")
  }

  private def lowerReturnItemAst(items: ast.ReturnItem)(env: LowerEnv): ExprAs = items match {
    case ast.AliasedReturnItem(expression, variable) => expression match {
      case expressions.FunctionInvocation(_, expressions.FunctionName("collect"), _, _) => null // TODO: change this to None, return Option type and use flatMap to eliminate any None
      case _ => ExprAs(lowerExprAst(expression)(env), lowerExprAst(variable)(env).asInstanceOf[Var])
    }
    case ast.UnaliasedReturnItem(expr, inputText) => ExprAs(lowerExprAst(expr)(env), Var(inputText))
    case unknown => throw new Exception(s"$unknown is not supported in ReturnItem AST lowering")
  }

  private def getGroupKeysInReturn(returnItems: ast.ReturnItems): List[Var] = {
    returnItems.items.flatMap(returnItem => returnItem match {
      case ast.AliasedReturnItem(expression, _) => expression match {
        case expressions.Variable(name) => Some(Var(name))
        case expressions.Property(expressions.Variable(name), _) => Some(Var(name))
        case _ => None
      }
      case ast.UnaliasedReturnItem(expr, _) => expr match {
        case expressions.Variable(name) => Some(Var(name))
        case expressions.Property(expressions.Variable(name), _) => Some(Var(name))
        case _ => None
      }
      case _ => None
    }).toList.distinct
  }
  private def existAggregator(returnItems: ast.ReturnItems): Boolean = {
    // Return true if there exist aggregator like Count(), Sum(), ... in the Return Clause
    val aggregator_names = List("count", "sum")
    returnItems.items.collectFirst{
      case ast.AliasedReturnItem(expressions.FunctionInvocation(_, expressions.FunctionName(funcname), _, _), _)
        if aggregator_names.contains(funcname) => true
      case ast.UnaliasedReturnItem(expressions.FunctionInvocation(_, expressions.FunctionName(funcname), _, _), _)
        if aggregator_names.contains(funcname) => true
    } match {
      case Some(v) => true
      case None => false
    }
  }

  private def lowerExprAst(expr: util.ASTNode)(env: LowerEnv): Expr = expr match {
    case expressions.SignedDecimalIntegerLiteral(value) => Value(value.toLong)
    case expressions.StringLiteral(value) => Value(value)
    case expressions.Variable(name) => Var(name)
    case expressions.PropertyKeyName(name) => Var(name)
    case expressions.Property(vrb1, vrb2) => Key(lowerExprAst(vrb1)(env).asInstanceOf[Var], lowerExprAst(vrb2)(env).asInstanceOf[Var])
    case expressions.And(left, right) => Binop("AND", lowerExprAst(left)(env), lowerExprAst(right)(env))
    case expressions.Or(left, right) => Binop("OR", lowerExprAst(left)(env), lowerExprAst(right)(env))
    case expressions.Not(expr) => expr match {
      case expressions.Equals(key, value) => desugarCmpOrder(Cmp("!=", lowerExprAst(key)(env), lowerExprAst(value)(env)))
      case expressions.In(expr, expressions.Variable(vrb)) => In(lowerExprAst(expr)(env), Var(vrb), neg = true)
      case other => Not(lowerExprAst(other)(env)) // including path variable
    }
    case expressions.Equals(key, value) => desugarCmpOrder(Cmp("=", lowerExprAst(key)(env), lowerExprAst(value)(env)))
    case expressions.NotEquals(key1, key2) => desugarCmpOrder(Cmp("!=", lowerExprAst(key1)(env), lowerExprAst(key2)(env)))
    case expressions.Ands(List(compare1, compare2)) => Binop("AND", lowerExprAst(compare1)(env), lowerExprAst(compare2)(env))
    case expressions.LessThan(key, value) => desugarCmpOrder(Cmp("<", lowerExprAst(key)(env), lowerExprAst(value)(env)))
    case expressions.GreaterThan(key, value) => desugarCmpOrder(Cmp(">", lowerExprAst(key)(env), lowerExprAst(value)(env)))
    case expressions.GreaterThanOrEqual(key, value) => desugarCmpOrder(Cmp(">=", lowerExprAst(key)(env), lowerExprAst(value)(env)))
    case expressions.In(expr, expressions.ListLiteral(varList)) => {
      val e = lowerExprAst(expr)(env)
      val cyphVarList = varList.map {
        case expressions.Variable(name) => Var(name)
        case unknown => throw new Exception(s"$unknown is not supported in IN [ ... ] lowering")
      }
      val initValue = desugarCmpOrder(Cmp("=", e, cyphVarList(0)))
      cyphVarList.drop(1).foldLeft[Expr](initValue)((acc, cur) => Binop("OR", acc, Cmp("=", e, cur)))
    }
    case expressions.In(expr, expressions.Variable(vrb)) => In(lowerExprAst(expr)(env), Var(vrb), neg = false)
    case expressions.CaseExpression(expr, whenThen, Some(elseCase)) => {
      val exprValue = expr match {
        case Some(value) => Some(lowerExprAst(value)(env))
        case None => None
      }
      // De-sugar of Case Expression : CASE expr WHEN true THEN expr2 ELSE ... => de-sugar to CASE WHEN expr THEN expr2 ELSE ...
      whenThen(0)._1 match {
        case expressions.True() => Case(None, exprValue.get, lowerExprAst(whenThen(0)._2)(env), lowerExprAst(elseCase)(env))
        case whenExpr => Case(exprValue, lowerExprAst(whenExpr)(env), lowerExprAst(whenThen(0)._2)(env), lowerExprAst(elseCase)(env))
      }
    }
    case expressions.IsNull(e) => IsNULL(lowerExprAst(e)(env))
    case expressions.Null() => Value("NULL")
    case expressions.Add(e1, e2) => Cmp("+", lowerExprAst(e1)(env), lowerExprAst(e2)(env))
    case expressions.Subtract(e1, e2) => Cmp("-", lowerExprAst(e1)(env), lowerExprAst(e2)(env))
    case expressions.FunctionInvocation(_, expressions.FunctionName("sum"), distinct, args) => AggSum(lowerExprAst(args(0))(env), env.curGroupBy)
    case expressions.FunctionInvocation(_, expressions.FunctionName("min"), distinct, args) => AggMin(lowerExprAst(args(0))(env))
    case expressions.FunctionInvocation(_, expressions.FunctionName("length"), distinct, args) => args(0) match {
      case expressions.Variable(pathVar) => LengthPath(Left(Var(pathVar))) // Var(pathVar) will be substituted
      case other => throw new Exception(s"$other is not supported in length(...)")
    }
    case expressions.FunctionInvocation(_, expressions.FunctionName("count"), distinct, args) => {
      val countArg = if (distinct) {
        Distinct(lowerExprAst(args(0))(env))
      } else {
        lowerExprAst(args(0))(env)
      }
      AggCount(countArg, env.curGroupBy)
    }
    case expressions.FunctionInvocation(_, expressions.FunctionName("size"), distinct, args) => {
      val argExpr = lowerExprAst(args(0))(env)
      val finalArg = if (distinct) {
        Distinct(argExpr)
      } else {
        argExpr
      }
      AggSize(finalArg)
    }
    case expressions.ListComprehension(expressions.ExtractScope(variable, Some(predicate), _), ref) => {
      val (var_attr, refTable) = (variable, ref) match {
        case (expressions.Variable(vrb), expressions.Variable(table)) => (Var(vrb), Var(table))
        case (other1, other2) => throw new Exception(s"$other1 IN $other2 can not be lowered")
      }
      ListComp(var_attr, refTable, lowerExprAst(predicate)(env), env.curGroupBy)
    }
    case expressions.PatternExpression(expressions.RelationshipsPattern(relaPatt)) => relaPatt match {
      case chain: expressions.RelationshipChain => {
        val (graphPttns, properties) = lowerPatternAstHelper(chain)(env)
        val constraint = propertiesToSingleExpr(properties)
        PatternExpr(graphPttns, constraint)
      }
      case unknown => throw new Exception(s"$unknown is not supported in List Comprehension lowering")
    }
    case expressions.True() => Value("True")
    case expressions.False() => Value("False")
    case unknown => throw new Exception(s"$unknown is not supported in Expression AST lowering")
  }

  def desugarCmpOrder(expr: Expr): Expr = expr match {
    case Cmp("<", Value(v), e) => Cmp(">", e, Value(v))
    case Cmp(">", Value(v), e) => Cmp("<", e, Value(v))
    case Cmp(">=", Value(v), e) => Cmp("<=", e, Value(v))
    case Cmp("<=", Value(v), e) => Cmp(">=", e, Value(v))
    case Cmp("=", Value(v), e) => Cmp("=", e, Value(v))
    case Cmp("!=", Value(v), e) => Cmp("!=", e, Value(v))
    case other => other
  }

  private def lowerMatchAst(pattern: expressions.Pattern)(where: Option[ast.Where])(option: Boolean)(env: LowerEnv): (List[Clause], LowerEnv) = {
    val (patternList, propertyList, nenv) = lowerPatternAst(pattern)(env)
    val matchClause = patternList.map(p => Match(p))
    val whereClause = lowerWhereAst(where)(propertyList)(env) match {
      case Some(whereCl) => List(whereCl)
      case None => List()
    }
    val paths = nenv.binding.values.toList
    val matchAndWhereAndSP = matchClause ++ whereClause ++ paths.map {
      case s: ShortestPath => s
    }
    val updatedEnv = LowerEnv(env.binding ++ nenv.binding, List())
    if (option) {
      (List(OptionalMatch(matchAndWhereAndSP)), updatedEnv)
    } else {
      (matchAndWhereAndSP, updatedEnv)
    }
  }

  private def lowerWhereAst(where: Option[ast.Where])(property: List[Expr])(env: LowerEnv): Option[Clause] = {
    val propertyExpr = propertiesToSingleExpr(property)
    (where, propertyExpr) match {
      case (Some(ast.Where(expr1)), Some(expr2)) => Some(Where(Binop("AND", lowerExprAst(expr1)(env), expr2)))
      case (Some(ast.Where(expr1)), None) => Some(Where(lowerExprAst(expr1)(env)))
      case (None, Some(expr)) => Some(Where(expr))
      case (None, None) => None
    }
  }

  private def lowerWithDistinctAst(returnItems: ast.ReturnItems)(where: Option[ast.Where])(env: LowerEnv): (List[Clause], LowerEnv) = returnItems match {
    case ast.ReturnItems(_, items) => {
      /* for now, WITH DISTINCT only appears without having aggregations in the clause,
      TODO: figure out aggregation semantics for WITH DISTINCT later, for now
      we only assume forms of WITH DISTINCT VAR1, VAR2, VAR3...
       */
      val ReturnItems = items.map(lowerReturnItemAst(_)(env)).filterNot(_ == null).toList
      val withDistinctClause = WithDistinct(ReturnItems)
      val whereClause = lowerWhereAst(where)(List())(env) match {
        case Some(whereCl) => List(whereCl)
        case None => List()
      }
      val withAndWhere = withDistinctClause +: whereClause
      (withAndWhere, env)
    }
    case other => throw new Exception(s"$other is not supported in WITH DISTINCT")
  }

  private def lowerWithAst(returnItems: ast.ReturnItems)(where: Option[ast.Where])(env: LowerEnv): (List[Clause], LowerEnv) = returnItems match {
    case ast.ReturnItems(_, items) => {
      // (a) Store groupkeys (for aggregation like sum(), count())
      // and update curGroupBy of env
      val groupByKeys = items.flatMap(e => e match {
        case ast.UnaliasedReturnItem(expr, inputText) => Some(Var(inputText))
        case ast.AliasedReturnItem(expr, variable) => expr match {
          case expressions.Property(expr2, _) => expr2 match {
            case expressions.Variable(vrb) => Some(Var(vrb))
            case _ => None
          }
          case _ => None
        }
        case _ => None
      }).toList

      val localEnv = LowerEnv(env.binding, groupByKeys)
      // (b) Separate lowering logic between collect(...) and others
      // Get grouping keys A1, A2 ... from ...AS A1, ...AS A2,
      val normalReturnItems = items.map(lowerReturnItemAst(_)(localEnv)).filterNot(_ == null).toList
      // In the case of WITH univ (where univ is a list), remove this from WITH,
      // This requires us to store any names of lists in the environment
      // Note: withGroupKeys should not include univ
      val normalReturnItemsWithoutCollection = normalReturnItems.filter(eas => !localEnv.binding.contains(eas.a))
      val withGroupKeys = normalReturnItemsWithoutCollection.map(eas => eas.a)
      val withCollects = items.map(c => lowerCollectAst(c)(withGroupKeys)(env)).filterNot(_ == null).toList
      // Add new collectList name to environment
      val newCollecList = withCollects.map {
        case WithCollect(expr, attribute, relation, groupKey, _) => relation
      }.map(vrb => (vrb -> null))

      val nenv = LowerEnv(localEnv.binding ++ newCollecList, localEnv.curGroupBy)

      val finalWithClause = normalReturnItemsWithoutCollection match {
        case Nil => withCollects
        case _ => withCollects :+ With(normalReturnItemsWithoutCollection)
      }
      val whereClause = lowerWhereAst(where)(List())(env) match {
        case Some(whereCl) => List(whereCl)
        case None => List()
      }
      val withAndWhere = finalWithClause ++ whereClause
      (withAndWhere, nenv)
    }
    case unknown => throw new Exception(s"$unknown is not supported in With AST lowering")
  }

  private def lowerUnwindAst(expr: expressions.Expression, variable: expressions.Variable)(accClause: List[Clause]): List[Clause] = expr match {
    // in place updates
    case expressions.Variable(vrbName) => {
      val relationName = vrbName
      val existWithCollect = accClause.exists {
        case WithCollect(_, attribute, Var(vrbName), _, _) => true
        case _ => false
      }
      if (existWithCollect) {
        accClause.map {
          case WithCollect(expr, attr, Var(name), keys, _) => {
            if (name == relationName) {
              WithCollect(expr, Var(variable.name), Var(name), keys, true)
            } else {
              WithCollect(expr, attr, Var(name), keys, true)
            }
          }
          case otherClause => otherClause
        }
      } else {
        throw new Exception(s"matching WithCollect of UNWIND is not found")
      }
    }
    case _ => throw new Exception(s"matching WithCollect is not found for UNWIND $expr")
  }

  private def lowerCollectAst(item: ast.ReturnItem)(groupKey: List[Var])(env: LowerEnv): Clause = item match {
    case ast.AliasedReturnItem(expressions.FunctionInvocation(_, expressions.FunctionName("collect"), _, args), variable) =>
      WithCollect(lowerExprAst(args(0))(env), Var(freshAttribute(variable.name)), Var(variable.name), groupKey, false)
    case _ => null
  }

  private def propertiesToSingleExpr(property: List[Expr]): Option[Expr] = property match {
    case Nil => None
    case head :: Nil => Some(head)
    case _ => Some(Binop("AND", propertiesToSingleExpr(property.init).get, property.last))
  }

  private def lowerPatternAst(pattern: expressions.Pattern)(env: LowerEnv): (List[Pattern], List[Expr], LowerEnv) = pattern match {
    case expressions.Pattern(pathList) => {
      val out = pathList.map {
        case expressions.EveryPath(element) => {
          val (pattern, condition) = lowerPatternAstHelper(element)(env)
          (pattern, condition, Map())
        }
        /* shortest path:
        * update lowering env only
        * */
        case expressions.NamedPatternPart(expressions.Variable(variable), expressions.ShortestPaths(pttnElem, _)) => {
          val (patterns, conditions) = lowerPatternAstHelper(pttnElem)(env)
          val condExpr = propertiesToSingleExpr(conditions)
          val graphPattn = PatternExpr(patterns, condExpr)
          val pathClause = ShortestPath(graphPattn, Var(variable))
          (List(), List(), Map(Var(variable) -> pathClause))
        }
        case unknown => throw new Exception(s"$unknown is not supported in Pattern AST lowering")
      }
      val (pttnList, condList, envMap) = out.foldLeft[(List[Pattern], List[Expr], Map[Var, Clause])]((List(), List(), Map()))(
        (acc, cur) => (acc._1 ++ cur._1, acc._2 ++ cur._2, acc._3 ++ cur._3))
      (pttnList, condList, LowerEnv(envMap, List()))
    }
    case unknown => throw new Exception(s"$unknown is not supported in Pattern AST lowering")
  }

  private def lowerPatternAstHelper(pattern: expressions.PatternElement)(env: LowerEnv): (List[Pattern], List[Expr]) = {
    val (patternList, propertyList, _) = lowerPatternAstCore(pattern)(env)
    val newPatternList = patternList.size match {
      case 0 => throw new Exception("Zero graph pattern is matched, please check your query")
      case 1 => patternList
      case _ => patternList.drop(1)
    }
    (newPatternList, propertyList)
  }

  private def lowerPatternAstCore(pattern: expressions.PatternElement)(env: LowerEnv): (List[Pattern], List[Expr], NodePattern) = pattern match {
    case expressions.NodePattern(name, labels, property, _) => {
      val vrbName = lowerVariableAst(name)
      val label = labels match {
        case List(lbl) => lbl.name
        case _ => throw new Exception("Multiple labels are not supported in Pattern AST lowering")
      }
      val constrains = lowerPropertyAst(vrbName, property)(env)
      (List(NodePattern(vrbName, label)), constrains, NodePattern(vrbName, label))
    }
    case expressions.RelationshipChain(src, edge, trg) => {
      val (prevPatterns, constrLeft, srcNode) = lowerPatternAstCore(src)(env)
      val (_, constrRight, trgNode) = lowerPatternAstCore(trg)(env)
      val (pattern, constrEdge) = edge match {
        case expressions.RelationshipPattern(edgeVrb, List(label), length, property, direction, _, _) => {
          val vrbName = lowerVariableAst(edgeVrb)

          val dist = length match {
            case Some(Some(expressions.Range(Some(lower), None))) =>
              Some(TransDistance(lower.stringVal.toInt))
            case Some(Some(expressions.Range(Some(lower), Some(upper)))) =>
              Some(NonTransDistance(lower.stringVal.toInt, upper.stringVal.toInt))
            case Some(Some(expressions.Range(None, Some(upper)))) =>
              Some(NonTransDistance(1, upper.stringVal.toInt))
            case Some(None) => Some(TransDistance(1))
            case None => None
            case _ => None
          }

          val constraint = lowerPropertyAst(vrbName, property)(env)

          val edgePattern = direction match {
            case expressions.SemanticDirection.OUTGOING =>
              EdgePattern(srcNode, trgNode, vrbName, label.name, dist, "direct")
            case expressions.SemanticDirection.INCOMING =>
              EdgePattern(trgNode, srcNode, vrbName, label.name, dist, "direct")
            case expressions.SemanticDirection.BOTH =>
              EdgePattern(srcNode, trgNode, vrbName, label.name, dist, "undirect")
          }
          (edgePattern, constraint)
        }
        case unknown => throw new Exception(s"$unknown is not supported in Relationship Pattern AST lowering")
      }

      (prevPatterns :+ pattern, constrLeft ++ constrEdge ++ constrRight, trgNode)
    }
    case unknown => throw new Exception(s"$unknown is not supported in Pattern AST lowering")
  }

  private def lowerPropertyAst(variable: Var, property: Option[expressions.Expression])(env: LowerEnv): List[Expr] = property match {
    case Some(expressions.MapExpression(items)) => lowerPropertyAstHelper(items)(variable)(env)
    case None => List()
  }

  private def lowerPropertyAstHelper(items: Seq[(expressions.PropertyKeyName, expressions.Expression)])
                                    (variable: Var)(env: LowerEnv): List[Expr] = items match {
    case Seq((keyName, value)) => List(Cmp("=", Key(variable, lowerExprAst(keyName)(env).asInstanceOf[Var]), lowerExprAst(value)(env)))
    case head :: body => List(Cmp("=", Key(variable, lowerExprAst(head._1)(env).asInstanceOf[Var]), lowerExprAst(head._2)(env))) ++ lowerPropertyAstHelper(body)(variable)(env)
  }

  private def lowerVariableAst(variable: Option[expressions.LogicalVariable]): Var = variable match {
    case Some(vrb) => Var(vrb.name)
    case None => Var(freshName())
  }

  var globalVarId = 0

  def freshName(): String = {
    globalVarId += 1
    s"x${globalVarId}"
  }

  def freshAttribute(name: String): String = {
    s"${name}_attr"
  }

  def resetGlobalId(): Unit = {
    globalVarId = 0
  }
}