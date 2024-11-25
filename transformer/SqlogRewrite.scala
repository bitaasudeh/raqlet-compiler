package uk.ac.ed.dal
package raqlet
package transformer

import ir._

object SqlogRewrite {
  def rewrite(query: DatalogQuery)(env: SqlogEnv): SqlogProg = {
    val groupedRules = groupRules(query.rules)
    val (sqlogRules, _) = groupedRules.foldLeft((List[SqlogElement](), env)) {
      (acc, cur) => {
        val (rewritten, nnenv) = rewriteRules(cur)(acc._2)
        (acc._1 :+ rewritten, nnenv)
      }
    }

    SqlogProg(sqlogRules)
  }

  /** Groups consecutive Decl and Def items into sublists.
   * Each sublist should start with a Decl and include
   * subsequent Def items until the next Decl is encountered.
   */
  def groupRules(rules: List[Rule]): List[List[Rule]] = {
    rules.foldLeft(List[List[Rule]]()) {
      (acc, rule) =>
        rule match {
          case d: Decl => acc :+ List(d)
          case f: Def => acc match {
            case front :+ last => front :+ (last :+ f)
            case Nil => throw new Exception(s"Def ${f} does not have preceding Decl")
          }
        }
    }
  }

  /** Rewrite rules within a group [Decl, Def, Def, ..] -> this one thing needs only one view name
   * */
  def rewriteRules(rules: List[Rule])(env: SqlogEnv): (SqlogElement, SqlogEnv) = {
    val (rewrittenRules, updatedEnv) = if (isRecursive(rules)) {
      rewriteRecursiveRules(rules)(env)
    } else {
      rewriteNonRecursiveRules(rules)(env)
    }
    (rewrittenRules, updatedEnv)
  }

  def isRecursive(rules: List[Rule]): Boolean = {
    rules.head match {
      case d: Decl => d.a.startsWith("TC")
      case o => throw new Exception(s"Expecting Decl but got ${o}")
    }
  }

  def rewriteRecursiveRules(rules: List[Rule])(env: SqlogEnv): (SqlogElement, SqlogEnv) = {
    val decl = rules.head.asInstanceOf[Decl]
    val (baseList, recurList) = _seperateBaseAndRecurCase(rules)
    val viewName = freshViewName()
    val updatedTableAlias = env.tableAlias + (SqlogVar(decl.a) -> SqlogVar(viewName))
    val nenv = SqlogEnv(updatedTableAlias, env.varOccur, env.unboundVars, env.inlineValues, env.schema)
    val (baseCase, nnenv) = rewriteRulesHelper(baseList)(needViewName = false)(nenv) // nenv used to retrieve updated schema that includes decl
    val (recurCase, _) = rewriteRulesHelper(recurList)(needViewName = false)(nenv)

    val argList = decl.args.map(arg => SqlogVar(arg.x)).map(variable => SqlogExprAs(variable, variable.name))
    val blockHead = SqlogHead(SqlogVar(viewName), argList)

    val updatedEnv = SqlogEnv(updatedTableAlias, env.varOccur, env.unboundVars, env.inlineValues, nnenv.schema)
    (RecursiveBlock(baseCase, recurCase, blockHead), updatedEnv)
  }

  def _seperateBaseAndRecurCase(rules: List[Rule]): (List[Rule], List[Rule]) = {
    val decl = rules.head
    val recurIdentifyList = rules.tail.map {
      case d: Def => {
        val tcName = d.a.a
        d.b.exists {
          case Relation(`tcName`, _) => true
          case _ => false
        }
      }
    }
    val recurRules = rules.tail.zip(recurIdentifyList).collect {
      case (rule, true) => rule
    }
    val baseRules = rules.tail.zip(recurIdentifyList).collect {
      case (rule, false) => rule
    }
    (decl +: baseRules, decl +: recurRules)
  }

  def rewriteNonRecursiveRules(rules: List[Rule])(env: SqlogEnv): (SqlogElement, SqlogEnv) = {
    val (queryBlock, updatedEnv) = rewriteRulesHelper(rules)(needViewName = true)(env)
    (queryBlock, updatedEnv)
  }

  def rewriteRulesHelper(rules: List[Rule])(needViewName: Boolean)(env: SqlogEnv): (SqlogElement, SqlogEnv) = {
    val decl = rules.head match {
      case d: Decl => d
      case o => throw new Exception(s"Expecting Decl but got ${o}")
    }
    val initEnv = SqlogEnv(env.tableAlias, env.varOccur, env.unboundVars, env.inlineValues, DatalogSchema(env.schema.schema :+ Left(decl)))
    val defs = rules.tail // List(Def1, Def2 ...)
    val headName = if (needViewName) {
      SqlogVar(freshViewName())
    } else {
      SqlogVar("-")
    }
    val (blocks, finalEnv) = defs.foldLeft((List[SqlogRule](), initEnv)) {
      (acc, cur) =>
        cur match {
          case d: Def => {
            val partialResetEnv = SqlogEnv(acc._2.tableAlias, Map(), List(), Map(), acc._2.schema) // Reset variable to R1.attr mapping upon every new Def
            val (block, newenv) = rewriteSingleRule(d)(headName)(partialResetEnv)
            (acc._1 :+ block, newenv)
          }
          case o => throw new Exception(s"Expecting Def but got ${o}")
        }
    }
    blocks match {
      case block :: Nil => (RuleBlock(block), finalEnv) // blocks is a list of single item (no union)
      case _ => (RuleUnionBlock(blocks), finalEnv) // otherwise (union)
    }
  }

  def getUnboundedVars(rule: Def): List[SqlogVar] = {
    val varsInHead = rule.a.x
    val varsInRelationAccess = rule.b.flatMap {
      case Relation(_, x) => x
      case _ => Nil
    }.distinct
    varsInHead.filterNot(varsInRelationAccess.contains).map(SqlogVar)
  }

  def rewriteSingleRule(rule: Def)(viewName: SqlogVar)(env: SqlogEnv): (SqlogRule, SqlogEnv) = {
    // reset table id first.
    resetTableid()
    // deal with unbounded variable here, and update in the env could do!
    val unboundVars = getUnboundedVars(rule)
    val initEnv = SqlogEnv(env.tableAlias, env.varOccur, unboundVars, env.inlineValues, env.schema) // update unbounded vars
    val (atomList, finEnv) = rule.b.foldLeft((List[SqlogAtom](), initEnv)) {
      (acc, curAtom) =>
        curAtom match {
          case r: Relation => {
            val (natom, nenv) = rewriteRelation(r)(acc._2)
            (acc._1 :+ natom, nenv)
          }
          // sum
          case CmpRel("=", DlVar(targetVar), s: Sum) => {
            val (natom, nenv) = rewriteSum(targetVar, s)(acc._2)
            (acc._1 :+ natom, nenv)
          }
          // Count
          case CmpRel("=", DlVar(alias), c: Count) => {
            val (natom, nenv) = rewriteCount(alias, c.args)(acc._2)
            (acc._1 :+ natom, nenv)
          }
          // Min
          case CmpRel("=", DlVar(targetVar), m: Min) => {
            val (natom, nenv) = rewriteMin(targetVar, m)(acc._2)
            (acc._1 :+ natom, nenv)
          }
          case c: CmpRel => {
            val (natom, nenv) = rewriteCmpRel(c)(acc._2)
            (acc._1 :+ natom, nenv)
          }
          // todo: to support more features, i.e. aggregation
          case _ => (acc._1, acc._2) // ignore not supported atom!
        }
    }
    // rewrite negation (treated separately because it needs all join conditions to construct where)
    val updatedAtomlist = rule.b.flatMap(atom => rewriteNegation(atom)(finEnv))
    // construct join condition from env vars Occur & add it to atomList
    val joinConds = constructJoinCondition(finEnv)
    val atomsInBody = atomList.filterNot(_ == NoneAtom) ++ updatedAtomlist
    // construct head
    val easInHead = rule.a.x.map(vrb => {
      val varkey = SqlogVar(vrb)
      (finEnv.inlineValues.get(varkey), finEnv.varOccur.get(varkey)) match {
        // if inline and varOccur happens to have same key, prioritise inline, this logic is developed for w = w : min .. use case from CQ1
        case (Some(value), _) => SqlogExprAs(value, vrb)
        case (_, Some(listOfValue)) => SqlogExprAs(listOfValue.head, vrb)
        case (None, None) => throw new Exception(s"Variable $vrb is not found in environment, something is wrong!")
      }
    })

    val head = SqlogHead(viewName, easInHead)
    val body = SqlogBody(atomsInBody ++ joinConds)
    // only updates tableAlias when viewName is not "-", this is due to merging logic of translating recursive & non-recursive rules
    val updatedTableAlias = viewName match {
      case SqlogVar("-") => finEnv.tableAlias
      case name => finEnv.tableAlias + (SqlogVar(rule.a.a) -> name)
    }
    // Use atomList to construct bodies and use finEnv info to
    (SqlogRule(head, body), SqlogEnv(updatedTableAlias, finEnv.varOccur, finEnv.unboundVars, finEnv.inlineValues, finEnv.schema))
  }


  // Rewrite Sum to SqlogSum
  def rewriteSum(target: String, sumAtom: Sum)(env: SqlogEnv): (SqlogAtom, SqlogEnv) = {
    // todo: new implementation of sum
    val relationToSum = sumAtom.atom match {
      case r: Relation => r
      case _ => throw new Exception("No relation is found in the sum atom")
    }
    val table: SqlogTable = env.tableAlias.get(SqlogVar(relationToSum.a)) match {
      case Some(viewName) => ViewTable(viewName)
      case None => TableAlias(SqlogVar(relationToSum.a), SqlogVar(freshTableName()))
    }
    val tableNameVar = table match {
      case ViewTable(name) => name
      case TableAlias(_, name) => name
    }
    val sumResultTableAlias = SqlogVar(freshTableName())

    val (sumTarget, propertyList, joinEquals) = relationToSum.x.zipWithIndex.foldLeft(
      (List[SqlogProperty](), List[SqlogExprAs](), List[SqlogAtom]())) {
      (acc, cur) =>
        cur._1 match {
          case "_" => (acc._1, acc._2, acc._3)
          case sumAtom.arg => {
            val attrname = getAttributeName(SqlogVar(relationToSum.a))(cur._2)(env.schema)
            val sumTargetList = acc._1 :+ SqlogProperty(tableNameVar, attrname)
            (sumTargetList, acc._2, acc._3)
          }
          case vrb: String => {
            env.varOccur.get(SqlogVar(vrb)) match {
              case Some(existingList) => {
                val attrname = getAttributeName(SqlogVar(relationToSum.a))(cur._2)(env.schema)
                val newJoin = JoinCondition(existingList.head, SqlogProperty(sumResultTableAlias, attrname))
                val updatedPropertyList = acc._2 :+ SqlogExprAs(SqlogProperty(tableNameVar, attrname), vrb)
                (acc._1, updatedPropertyList, acc._3 :+ newJoin)
              }
              case None => {
                (acc._1, acc._2, acc._3)
              }}}}
    }
    val sumConstruct = SqlogSum(SqlogExprAs(sumTarget.head, target), propertyList, joinEquals, table, sumResultTableAlias)
    val nenv = SqlogEnv(env.tableAlias, env.varOccur, env.unboundVars,
      env.inlineValues + (SqlogVar(target) -> SqlogCoalesce(SqlogProperty(sumResultTableAlias, SqlogVar(target)), SqlogIntValue(0))), env.schema)
    (sumConstruct, nenv)
  }

  // Rewrite Min to SqlogMin
  def rewriteMin(target: String, minAtom: Min)(env: SqlogEnv): (SqlogAtom, SqlogEnv) = {
    // relation to take min
    val relationToMin = minAtom.atom match {
      case r: Relation => r
      case _ => throw new Exception("No relation is found in the sum atom")
    }
    val table: SqlogTable = env.tableAlias.get(SqlogVar(relationToMin.a)) match {
      case Some(viewName) => ViewTable(viewName)
      case None => TableAlias(SqlogVar(relationToMin.a), SqlogVar(freshTableName()))
    }
    val tableNameVar = table match {
      case ViewTable(name) => name
      case TableAlias(_, name) => name
    }
    val minResultTableAlias = SqlogVar(freshTableName())
    val (propertyList, newVarOccur) = relationToMin.x.zipWithIndex.foldLeft((List[SqlogExprAs](), env.varOccur)) {
      (acc, cur) =>
        cur._1 match {
          case "_" | minAtom.arg => (acc._1, acc._2) // exclude min var in future join
          case vrb: String => {
            val attrname = getAttributeName(SqlogVar(relationToMin.a))(cur._2)(env.schema)
            val updatedPropertyList = acc._1 :+ SqlogExprAs(SqlogProperty(tableNameVar, attrname), vrb)
            val updatedVarList = acc._2.get(SqlogVar(vrb)) match {
              case Some(existingList) => existingList :+ SqlogProperty(minResultTableAlias, attrname)
              case None => List(SqlogProperty(minResultTableAlias, attrname))
            }
            (updatedPropertyList, acc._2.updated(SqlogVar(vrb), updatedVarList))
          }
        }
    }
    val minVar = SqlogExprAs(SqlogProperty(tableNameVar, SqlogVar(minAtom.arg)), target)
    val minConstruct = SqlogMin(minVar, propertyList, table, minResultTableAlias)
    val updatedEnv = SqlogEnv(env.tableAlias, newVarOccur, env.unboundVars,
      env.inlineValues + (SqlogVar(target) -> SqlogProperty(minResultTableAlias, SqlogVar(target))), env.schema)
    (minConstruct, updatedEnv)
  }

  // Rewrite Count to SqlogCount using left join and COALESCE logic
  def rewriteCount(countAssignedVar: String, atoms: List[Atom])(env: SqlogEnv): (SqlogAtom, SqlogEnv) = {

    val countRel = atoms.collectFirst { case r: Relation => r } match {
      case Some(r) => r
      case None => throw new Exception(s"No relation is found in count atom ${atoms}")
    }
    val relationName = SqlogVar(countRel.a)

    val subStmtViewName = SqlogVar(freshViewName())

    val table_left: SqlogTable = env.tableAlias.get(SqlogVar(countRel.a)) match {
      case Some(viewName) => ViewTable(viewName)
      case None => TableAlias(SqlogVar(countRel.a), SqlogVar(freshTableName()))
    }
    val table_right = ViewTable(SqlogVar(freshTableName()))

    val table_left_name = table_left match {
      case ViewTable(name) => name
      case TableAlias(_, name) => name
    }
    val table_right_name = table_right.view

    val initTuple = (List[SqlogExprAs](), env.varOccur, env.varOccur, List[SqlogAtom]())
    val (propertyList, newGlobalVarOccur, newLocalVarOccur, joinCond) = countRel.x.zipWithIndex.foldLeft(initTuple) {
      (acc, cur) => {
        val alias = cur._1
        val aliasVar = SqlogVar(alias)
        val attrname = getAttributeName(relationName)(cur._2)(env.schema)
        // check if an attribute in count has already appeared in previous rule
        val (joinConditions, selectColList) = acc._2.get(aliasVar) match {
          case Some(_) => {
            (acc._4 :+ JoinCondition(SqlogProperty(table_left_name, attrname), SqlogProperty(table_right_name, attrname)),
              acc._1 :+ SqlogExprAs(SqlogProperty(table_left_name, attrname), alias))
          }
          case None => (acc._4, acc._1)
        }
        val updatedGlobalVarList = acc._2.get(aliasVar) match {
          case Some(existingList) => existingList :+ SqlogProperty(subStmtViewName, aliasVar) // this is for WHERE in the CTE
          case None => List(SqlogProperty(subStmtViewName, aliasVar))
        }
        val updatedLocalVarList = acc._3.get(aliasVar) match {
          case Some(existingList) => existingList :+ SqlogProperty(table_left_name, attrname) // used for WHERE condition inside left join subquery
          case None => List(SqlogProperty(table_left_name, attrname))
        }
        (selectColList, acc._2.updated(SqlogVar(alias), updatedGlobalVarList), acc._3.updated(SqlogVar(alias), updatedLocalVarList), joinConditions)
      }
    }

    val updatedInlineVar = env.inlineValues + (SqlogVar(countAssignedVar) -> SqlogProperty(subStmtViewName, SqlogVar(countAssignedVar)))
    val updatedGlobalEnv = SqlogEnv(env.tableAlias, newGlobalVarOccur, env.unboundVars, updatedInlineVar, env.schema)
    val updatedLocalEnv = SqlogEnv(env.tableAlias, newLocalVarOccur, env.unboundVars, updatedInlineVar, env.schema)
    val filerConditions = atoms.flatMap {
      case c: CmpRel => Some(c)
      case _ => None
    }.map(atom => rewriteCmpRel(atom)(updatedLocalEnv)._1)

    val coalesceCount = SqlogProperty(table_right_name, SqlogVar(countAssignedVar))
    val countAlias = SqlogVar(countAssignedVar)
    val groupByVars = propertyList
    val leftJoinCondition = joinCond
    val whereCondition = filerConditions
    val finalViewName = subStmtViewName
    val countConstruct = SqlogCount(coalesceCount,countAlias, groupByVars, leftJoinCondition, whereCondition, finalViewName, table_left,table_right)

    (countConstruct, updatedGlobalEnv)
  }

  def rewriteNegation(atom: Atom)(env: SqlogEnv): Option[SqlogAtom] = atom match {
    case Neg(rel) => {
      val table: SqlogTable = env.tableAlias.get(SqlogVar(rel.a)) match {
        case Some(viewName) => ViewTable(viewName)
        case None => TableAlias(SqlogVar(rel.a), SqlogVar(freshTableName()))
      }
      val tableName = table match {
        case ViewTable(name) => name
        case TableAlias(_, name) => name
      }
      /* Obtain join conditions
         For each variables appearing in !R(var1, var2...), need to construct one join condition for each.
       */
      val joinCondList = rel.x.zipWithIndex.flatMap(vrb => vrb._1 match {
        case "_" => None
        case name => env.varOccur.get(SqlogVar(name)) match {
          case Some(vrbList) => {
            val attrname = getAttributeName(SqlogVar(rel.a))(vrb._2)(env.schema)
            val curAttr = SqlogProperty(tableName, attrname)
            val joinCond = JoinCondition(curAttr, vrbList.head)
            Some(joinCond)
          }
          case None => None
        }
      })

      Some(NegationAccess(table, joinCondList))
    }
    case _ => None
  }

  /** rewrite for example: Person_IS_LOCATED_IN_City(n, p, x42)
   * to [Person_IS_LOCATED_IN_City, V1](V1.id1, V1.id2, V1.id)
   * and update variable
   * n -> V1.id1
   * p -> V1.id2
   * x42 -> V1.id
   * Steps to take:
   * (1) check if Person_IS_LOCATED_IN_City is an original table or view
   * check env
   * (2) if not view, generate a new table name R1
   * (3) get the original attribute name of each non-wildcard field name from Datalog schema
   * (4) update variable name in env
   */
  def rewriteRelation(rel: Relation)(env: SqlogEnv): (SqlogAtom, SqlogEnv) = {
    val tableName: SqlogTable = env.tableAlias.get(SqlogVar(rel.a)) match {
      case Some(viewName) => ViewTable(viewName)
      case None => TableAlias(SqlogVar(rel.a), SqlogVar(freshTableName()))
    }
    val (propertyList, newVarOccur) = rel.x.zipWithIndex.foldLeft((List[SqlogProperty](), env.varOccur)) {
      (acc, cur) =>
        cur._1 match {
          case "_" => (acc._1, acc._2)
          case vrbname: String => {
            val attrname = getAttributeName(SqlogVar(rel.a))(cur._2)(env.schema)
            val tableNameVar = tableName match {
              case ViewTable(name) => name
              case TableAlias(_, name) => name
            }
            val updatedPropertyList = acc._1 :+ SqlogProperty(tableNameVar, attrname)
            val updatedVarList = acc._2.get(SqlogVar(vrbname)) match {
              case Some(existingList) => existingList :+ SqlogProperty(tableNameVar, attrname)
              case None => List(SqlogProperty(tableNameVar, attrname))
            }
            (updatedPropertyList, acc._2.updated(SqlogVar(vrbname), updatedVarList))
          }
        }
    }
    (RelationAccess(tableName, propertyList), SqlogEnv(env.tableAlias, newVarOccur, env.unboundVars, env.inlineValues, env.schema))
  }

  def rewriteArith(arith: Arith)(env: SqlogEnv): (SqlogAtom, SqlogEnv) = arith match {
    case Arith(op, e1, e2) => {
      val v1 = e1 match {
        case DlVar(var1) => var1
        case _ => ???
      }

      val propertyExpr = env.varOccur.get(SqlogVar(v1)) match {
        case Some(list) => list.head
        case None => throw new Exception(s"Variable $v1 not found in env")
      }

      e2 match {
        case Constant(num: Int) => (BinaryOp(op, propertyExpr, SqlogIntValue(num)), env)
        case Constant(long: Long) => (BinaryOp(op, propertyExpr, SqlogLongIntValue(long)), env)
        case Constant(str: String) => (BinaryOp(op, propertyExpr, SqlogStringValue(str)), env)
        case DlVar(var2) => {
          val propertyExpr2 = env.varOccur.get(SqlogVar(var2)) match {
            case Some(list) => list.head
            case None => throw new Exception(s"Variable $var2 not found in env")
          }
          (BinaryOp(op, propertyExpr, propertyExpr2), env)
        }
        case arith2: Arith => {
          val e2trans = rewriteArith(arith2)(env)._1
          (BinaryOp(op, propertyExpr, e2trans), env)
        }
        case o => throw new Exception(s"$o appears on the right side of CmpRel, please check")
      }
    }
  }

  /**
   * Given a ComRel atom, match on different cases and return rewritten atom
   */
  def rewriteCmpRel(cmp: CmpRel)(env: SqlogEnv): (SqlogAtom, SqlogEnv) = cmp match {
    /* two cases: bounded_var = 42, unbounded_var = 42
    */
    case CmpRel("=", var1, DlVar(var2)) => {
      val variable1 = var1 match {
        case DlVar(v) => SqlogVar(v)
        case _ => ???
      }
      val variable2 = SqlogVar(var2)

      if (var1 == var2) {
        (NoneAtom, env) // todo: should remove this case to Datalog rewritten
      } else if (env.unboundVars.contains(variable1)) {
        // unbounded variable on the left side case
        val propertyExpr = env.varOccur.get(variable2) match {
          case Some(e) => e.head
          case None => throw new Exception(s"Variable $variable2 not found in env")
        }
        (NoneAtom, SqlogEnv(env.tableAlias, env.varOccur, env.unboundVars,
          env.inlineValues + (variable1 -> propertyExpr), env.schema))
      } else if (env.unboundVars.contains(variable2)) {
        // unbounded variable on the right side case
        val propertyExpr = env.varOccur.get(variable1) match {
          case Some(e) => e.head
          case None => throw new Exception(s"Variable $variable1 not found in env")
        }
        (NoneAtom, SqlogEnv(env.tableAlias, env.varOccur, env.unboundVars,
          env.inlineValues + (variable2 -> propertyExpr), env.schema))
      } else {
        // both bounded
        (env.varOccur.get(variable1), env.varOccur.get(variable2)) match {
          case (None, None) => throw new Exception(s"Variables $variable1 and $variable2 not found in env")
          case (Some(e1), Some(e2)) => (BinaryOp("=", e1.head, e2.head), env)
          case (Some(e1), _) => {
            val updatedEnv = SqlogEnv(env.tableAlias, env.varOccur + (variable2 -> e1), env.unboundVars, env.inlineValues, env.schema)
            (NoopOp, updatedEnv)
          }
          case (_, Some(e2)) => {
            val updatedEnv = SqlogEnv(env.tableAlias, env.varOccur + (variable1 -> e2), env.unboundVars, env.inlineValues, env.schema)
            (NoopOp, updatedEnv)
          }
        }
//  old code logic to deal with implicit join such as: A(a, x), B(b, y), a = b.
//        val e1 = env.varOccur.get(variable1) match {
//          case Some(e) => e.head
////          case None => throw new Exception(s"Variable $variable1 not found in env")
//
//        }
//        val e2 = env.varOccur.get(variable2) match {
//          case Some(e) => e.head
//          case None => throw new Exception(s"Variable $variable2 not found in env")
//        }
//
//        (BinaryOp("=", e1, e2), env)
      }
    }
    case CmpRel("=", var1, value) => {
      val sqlogVar = var1 match {
        case DlVar(v) => SqlogVar(v)
        case _ => ???
      }
      val typedValue = value match {
        case Constant(num: Int) => SqlogIntValue(num)
        case Constant(long: Long) => SqlogLongIntValue(long)
        case Constant(str: String) => SqlogStringValue(str)
        case arith: Arith => rewriteArith(arith)(env)._1
      }

      if (env.unboundVars.contains(sqlogVar)) {
        // unbounded case return inline
        (NoneAtom, SqlogEnv(env.tableAlias, env.varOccur, env.unboundVars,
          env.inlineValues + (sqlogVar -> typedValue), env.schema))
      } else {
        // bounded case, return BinaryOp
        env.varOccur.get(sqlogVar) match {
          case Some(list) => (BinaryOp("=", list.head, typedValue), env)
          case None => throw new Exception(s"In atom ${cmp}, Variable alias ${var1} does not have actual attribute in env")
        }
      }
    }
    case CmpRel(op, e1, e2) => {
      val v1 = e1 match {
        case DlVar(var1) => var1
        case _ => ???
      }

      val propertyExpr = env.varOccur.get(SqlogVar(v1)) match {
        case Some(list) => list.head
        case None => throw new Exception(s"Variable $v1 not found in env")
      }

      e2 match {
        case Constant(num: Int) => (BinaryOp(op, propertyExpr, SqlogIntValue(num)), env)
        case Constant(long: Long) => (BinaryOp(op, propertyExpr, SqlogLongIntValue(long)), env)
        case Constant(str: String) => (BinaryOp(op, propertyExpr, SqlogStringValue(str)), env)
        case DlVar(var2) => {
          val propertyExpr2 = env.varOccur.get(SqlogVar(var2)) match {
            case Some(list) => list.head
            case None => throw new Exception(s"Variable $var2 not found in env")
          }
          (BinaryOp(op, propertyExpr, propertyExpr2), env)
        }
        case arith: Arith => {
          val e2trans = rewriteArith(arith)(env)._1
          (BinaryOp(op, propertyExpr, e2trans), env)
        }
        case o => throw new Exception(s"$o appears on the right side of CmpRel, please check")
      }
    }
    case other => throw new Exception(s"$other is not supported in rewriting CmpRel for now!")
  }

  def constructJoinCondition(env: SqlogEnv): List[SqlogAtom] = {
    env.varOccur.flatMap(vrb => vrb._2.length match {
      case len if len > 1 => vrb._2.sliding(2).map { case List(v1, v2) => JoinCondition(v1, v2) }.toList
      case _ => List()
    }).toList
  }

  def getAttributeName(rel: SqlogVar)(pos: Int)(schema: DatalogSchema): SqlogVar = {
    schema.schema.collectFirst {
      case Left(Decl(rel.name, args)) => args.apply(pos).x
    } match {
      case Some(attr) => SqlogVar(attr)
      case None => throw new Exception(s"Attribute at index $pos not found in ${rel.name}")
    }
  }

  var globalViewId = 0
  var globalTableId = 0

  def freshViewName(): String = {
    globalViewId += 1
    s"V${globalViewId}"
  }

  def freshTableName(): String = {
    globalTableId += 1
    s"R${globalTableId}"
  }

  def resetTableid(): Unit = {
    globalTableId = 0
  }
}