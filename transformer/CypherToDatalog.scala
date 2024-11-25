package uk.ac.ed.dal
package raqlet
package transformer

import ir._

/* Design decision documents

   Design decision of table
   table is used for storing information related to list collect() and
   shortestpath, store can be
   (1) Relation Name -> // attribute name (usecase: collect)// OLD DESIGN -> NEW DESIGN Relation
   (2) Attribute name -> Relation name (usecase: collect)
   (3) path variable name -> Relation (usecase: shortest path)
   ---
   (4) variable name -> attribute name (usecase: collect) NOTE: this info gets updated when processing WITHCOLLECT clause
   To capture this, we use
   Map[String, (String, Optional[Relation])] for now, needs refactoring later.


   Design decision of varInfo and varTypes, we created varInfo first to capture the label etc of
   a node or edge variable without thinking too much about how to carry around type information.
   Later we realised better keep track of all variables/aliases with their type info in the env,
   hence the new field varTypes is added to env. Ideally, we want to merge varInfo and varTypes with
   only necessary info during refactor.

   Why withFilter?
   To support bag semantics for WITH clause, we do not remove any variables from WITH during translation.
   However, this introduce a new problem that following clause may reuse previous variable (because they SHOULD become free
   after being projected away through WITH clause), but because we pass all variables for WITH, this causes variable
   name collision. withFilter is to keep track of variable names that should theoretically be eliminated thru WITH clause.
   this is to help resolve variable collisions.

   Why propertyRegistry?
   In case we have WHERE city.name = "aa", person.name = "bb" in the same Clause, property "name" collides, so we want to rename the proeprty name.

   Why collectVars?
   for use case like CQ10: if in WITH clause, a variable gets collected (into a list), then that variable should not be included in the
   argument scope of WITH(var1, var2, ...), ideally so do all the variables newly introduced together with that variable in a MATCH clause - they should be removed from the scope too!
   but...will get around to that later.

   Why unwindVar?
   it seems we need to differentiate variable that get collected and then unwinded & variable that get collected but not get unwinded
   So far this is treated in the same way through WITHCOLLECT(); now to differentiate which var is the unwinded var, we create this
   unwindVar field in CypherEnv that keeps track of unwinded var and its associated Relation (produced through collect(...))
 */
case class CypherEnv(prevRel: CypherPrevRel, varInfo: Map[Var, CypherVarEnv],
                     schema: CyphSchema, table: Map[String, (String, Option[Relation])], newRules: List[Rule],
                     varTypes: Map[Var, CyphType], withFilter: List[Var], propertyRegistry: Map[Var, Var],
                     collectVars: Map[Var, List[Var]], unwindVar: Map[String, Relation])

case class CypherVarEnv(label: LabelSpec, src: Option[Var], trg: Option[Var])

/* Design decisions for newargs:
(1) Since Datalog adopts set semantics whilst Cypher uses bag semantics, to avoid strange things happen when translation
WITH Var, aggregate(something), we preserve all variables (from previous clauses) when translating WITH. However, this will affect
translation of count(...); see complex 5, 6 , 12 as examples. So we need a field in the env keep track of both
(a) all previous variables (b) variables used in the prior clause.
But! Since we desugar MATCH ... to a sequence of MATCH ..., whenever we encounter MATCH, we keep all variables
* */
case class CypherPrevRel(args: List[Arg], rel: Option[Relation], newargs: List[Arg])




object CypherToDatalog {

  def translate(cq: CypherQuery)(env: CypherEnv): DatalogQuery = cq match {
    case Return(aliasedItems) => transReturn(aliasedItems)(env)
    case ClauseQuery(clause, query) => {
      val (rule1, nenv) = transClause(clause)(env)
      val rule2 = translate(query)(nenv)
      val rules = rule1.rules ++ rule2.rules
      DatalogQuery(rules)
    }
  }

  def transReturn(aliasedItems: List[ExprAs])(env: CypherEnv): DatalogQuery = {

    val (typedArgs, nenv) = aliasedItems.foldLeft[(List[Arg], CypherEnv)]((List(), env))((acc, cur) => {
      val (ty, accEnv) = getExprAsTypes(cur)(acc._2)
      (acc._1 :+ Arg(cur.a.strg, ty), accEnv)
    })

    val rule1 = Decl("Return", typedArgs)

    val untypedArgs = aliasedItems.map(attr => attr.a.strg) // Get A1, A2.. from __ AS A1, __ AS A2
    val defHead = Relation("Return", untypedArgs)
    // `Case When` has its own treatment during translation
    val newRuleBodyTupleList: List[(List[Rule], List[Atom])] = aliasedItems.map {
      case eas@ExprAs(c: Case, v) => transCaseAs(eas)(nenv)
      case eas => {
        val (atoms, nnenv) = transExprAs(eas)(nenv)
        (nnenv.newRules, atoms)
      }
    }
    val body = newRuleBodyTupleList.flatMap(e => e._2)
    val newRules = newRuleBodyTupleList.flatMap(e => e._1)
    val defFinalBody = env.prevRel.rel match {
      case Some(rel) => rel :: body
      case None => body
    }

    val rule2 = Def(defHead, defFinalBody)
    DatalogQuery(newRules ++ List(rule1, rule2))
  }

  def transClause(cl: Clause)(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    cl match {
      case Match(pattern) => transMatch(pattern)(weight = false)(env)
      case UnionMatch(pattern) => transUnionMatch(pattern)(weight = false)(env)
      case Where(expr) => transWhere(expr)(env)
      case With(exprAs) => transWith(exprAs)(env)
      case WithDistinct(exprAs) => transWithDistinct(exprAs)(env)
      case OptionalMatch(clauses) => transOptionalMatch(clauses)(env)
      case wc: WithCollect => transWithCollect(cl)(env)
      case sp: ShortestPath => transShortestPath(sp)(env)
      case unknown => throw new Exception(s"$unknown clause is not supported")
    }
  }

  def transShortestPath(sp: ShortestPath)(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    val (pathRules, localnenv) = sp.pattern.patterns.foldLeft[(List[Rule], CypherEnv)]((List(), env))((acc, cur) => {
      val (dLogQuery, nenv) = transMatch(cur)(weight = true)(acc._2)
      (acc._1 ++ dLogQuery.rules, nenv)
    })
    // Obtain Path(..., w)
    val pathRel = pathRules.last match {
      case Def(a, b) => a
      case _ => throw new Exception("Error: Relation for shortest path not found!")
    }
    // Generate ShortestPath(n1, n2, w)
    val spName = freshName("ShortestPath")
    val spDecl = Decl(spName, localnenv.prevRel.args)
    val spUntypedArgs = localnenv.prevRel.args.map(e => e.x)
    val spDefHead = Relation(spName, spUntypedArgs)
    val spDefBody = List(pathRel, CmpRel("=", DlVar("w"), Min("w", pathRel)))
    val spDef = Def(spDefHead, spDefBody)
    // Update Env
    (DatalogQuery(pathRules ++ List(spDecl, spDef)),
      CypherEnv(env.prevRel, env.varInfo, env.schema, env.table + (sp.pathVar.strg -> (spName, Some(spDefHead))), env.newRules,
        env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
    // TODO: not sure whether to pass newRules here or reset it!?
  }

  private def transMatch(pattern: Pattern)(weight: Boolean)(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    val prevClauseTypedArgs = env.prevRel.args
    val currClauseTypedArgsWithoutWeight = getMatchArgsHelper(pattern)(env) // Get new arguments for graph pattern
    val currClauseTypedArgs = if (weight) {
      currClauseTypedArgsWithoutWeight :+ Arg("w", CIntType)
    } else {
      currClauseTypedArgsWithoutWeight
    }

    val newTypedArgs = currClauseTypedArgs.diff(prevClauseTypedArgs)
    val refRelationList = newTypedArgs.filter(arg => env.unwindVar.contains(arg.x)).map(arg => env.unwindVar(arg.x)) // handel variable created by previous UNWIND clause

    // rename variables in previous relation whenever there is var collision
    val varInCurrClause = currClauseTypedArgs.map(arg => arg.x)
    val renamed_prev_rel = env.prevRel.rel match {
      case Some(relation) => {
        val fields = relation.x.map(vrb => if (varInCurrClause.contains(vrb) && env.withFilter.contains(Var(vrb))) {
          freshName(vrb)
        } else {vrb})
        List(Relation(relation.a, fields))
      }
      case None => List()
    }

    /* In the current design, getMatchArgsHelper does not consider variables
    that are already appeared, so we need to deduplicate variables (Args),
    hence the .diff()
    */
    val newMatchTypedArgs = (prevClauseTypedArgs ++ currClauseTypedArgs).distinct
    val relName = freshName("Match")
    val matchDecl = Decl(relName, newMatchTypedArgs)
    val matchDefHead = Relation(relName, newMatchTypedArgs.map(arg => arg.x))


    val newPrevRel = CypherPrevRel(newMatchTypedArgs, Some(matchDefHead), (env.prevRel.newargs ++ currClauseTypedArgs).distinct)

    pattern match {
      case NodePattern(vrb, label) => {
        val body = List(transNode(pattern.asInstanceOf[NodePattern])(env)) ++ refRelationList
        val matchDefBody = renamed_prev_rel ++ body
        val matchDef = Def(matchDefHead, matchDefBody)
        (DatalogQuery(List(matchDecl, matchDef)),
          CypherEnv(newPrevRel, env.varInfo + (vrb -> CypherVarEnv(LabelInput(label), None, None)),
            env.schema, env.table, env.newRules,
            env.varTypes ++ newMatchTypedArgs.map(arg => Var(arg.x) -> arg.t), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
      }
      case EdgePattern(src, trg, vrb, _, _, _) => {
        val (rules, relation) = transEdge(pattern.asInstanceOf[EdgePattern], weight)(env)
        val srcNodeRel = transNode(src)(env)
        val trgNodeRel = transNode(trg)(env)
        val body = List(relation, srcNodeRel, trgNodeRel) ++ refRelationList
        val matchDefBody = renamed_prev_rel ++ body
        val matchDef = Def(matchDefHead, matchDefBody)
        rules match {
          case None => (DatalogQuery(List(matchDecl, matchDef)),
            CypherEnv(newPrevRel, env.varInfo + (src.vrb -> CypherVarEnv(LabelInput(src.label), None, None))
              + (trg.vrb -> CypherVarEnv(LabelInput(trg.label), None, None))
              + (vrb -> CypherVarEnv(LabelInput(relation.a), Some(src.vrb), Some(trg.vrb))), env.schema, env.table, env.newRules,
              env.varTypes ++ newMatchTypedArgs.map(arg => Var(arg.x) -> arg.t), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
          case Some(tmpRules) => (DatalogQuery(tmpRules ++ List(matchDecl, matchDef)),
            CypherEnv(newPrevRel, env.varInfo + (src.vrb -> CypherVarEnv(LabelInput(src.label), None, None))
              + (trg.vrb -> CypherVarEnv(LabelInput(trg.label), None, None)), env.schema, env.table, env.newRules,
              env.varTypes ++ newMatchTypedArgs.map(arg => Var(arg.x) -> arg.t), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
        }
      }
    }
  }

  private def getNodeIdType(label: String)(env: CypherEnv): CyphType = {
    val schema = env.schema
    schema.types.collectFirst { case n: NodeType if (n.name == label) => n } match {
      case Some(n) => n.fields.find(_._1 == "id") match {
        case Some(a) => a._2
        case None => throw new Exception(s"id is not found in schema for node $label")
      }
      case None => throw new Exception(s"node with $label is not found in the schema")
    }
  }

  private def getEdgeIdType(srcLabel: String, trgLabel: String, edgeLabel: String)(env: CypherEnv): CyphType = {
    env.schema.types.collectFirst { case e: EdgeType if ((e.src.name == srcLabel) && (e.dest.name == trgLabel) && (e.rel.name == edgeLabel))
    => e
    } match {
      case Some(e) => e.fields.find(_._1 == "id") match {
        case Some(a) => a._2
        case None => throw new Exception(s"id is not found in schema for edge ${srcLabel}_${edgeLabel}_${trgLabel}")
      }
      case None => throw new Exception(s"Error: Edge label ${srcLabel}_${edgeLabel}_${trgLabel} is not found in the schema")
    }
  }

  private def getMatchArgsHelper(pattern: Pattern)(env: CypherEnv): List[Arg] = pattern match {
    case NodePattern(vrb, label) => List(Arg(vrb.strg, getNodeIdType(label)(env)))
    case EdgePattern(src, trg, vrb, edgeLabel, None, direct) => {
      val label = if (direct == "undirect") {
        edgeLabel ++ s"_undir"
      } else {
        edgeLabel
      }
      List(
        Arg(src.vrb.strg, getNodeIdType(src.label)(env)),
        Arg(vrb.strg, getEdgeIdType(src.label, trg.label, label)(env)),
        Arg(trg.vrb.strg, getNodeIdType(trg.label)(env))
      )
    }
    case EdgePattern(src, trg, _, _, Some(_), direct) => List(Arg(src.vrb.strg, getNodeIdType(src.label)(env)),
      Arg(trg.vrb.strg, getNodeIdType(trg.label)(env)))
  }

  def transNode(node: NodePattern)(env: CypherEnv): Relation = {
    val nodeIDB = lookupIdb(node.label)(env.schema)
    val numFields = numberOfAttr(nodeIDB)(env.schema)
    Relation(nodeIDB, node.vrb.strg :: List.fill(numFields)("_"))
  }

  def transEdge(edge: EdgePattern, weight: Boolean)(env: CypherEnv): (Option[List[Rule]], Relation) = {
    val relationshipLabel = edge.direction match {
      case "direct" => edge.src.label ++ "_" ++ edge.label ++ "_" ++ edge.trg.label
      case "undirect" => edge.src.label ++ "_" ++ edge.label ++ "_undir" ++ "_" ++ edge.trg.label
    }
    val edgeIDB = lookupIdb(relationshipLabel)(env.schema)
    val numFields = numberOfAttr(edgeIDB)(env.schema)

    edge.dist match {
      case None => {
        val edgeBody = List(edge.src.vrb.strg, edge.trg.vrb.strg, edge.vrb.strg) ++ List.fill(numFields)("_")
        (None, Relation(edgeIDB, edgeBody))
      }
      case Some(NonTransDistance(m1, m2)) => {
        // TODO: fix semantics for multiple matches for an edge label
        val tmpTypedArgsPreWeight = List(Arg(edge.src.vrb.strg, getNodeIdType(edge.src.label)(env)),
          Arg(edge.trg.vrb.strg, getNodeIdType(edge.trg.label)(env)))
        val tmpTypedArgs = if (weight) {
          tmpTypedArgsPreWeight :+ Arg("w", CIntType)
        } else {
          tmpTypedArgsPreWeight
        }
        val tmpUntypedArgs = tmpTypedArgs.map(e => e.x)
        val tmpName = freshName("NonTC")
        val tmpDecl = Decl(tmpName, tmpTypedArgs)
        val tmpRelHead = Relation(tmpName, tmpUntypedArgs)
        val tmpDefList: List[Def] = (m1 until (m2 + 1)).flatMap(num => {
          val listOfMultiEdgeRel: List[List[Atom]] = generateMultiEdgeRel(num, edge.src.vrb, edge.trg.vrb)(List(edgeIDB))(weight)(env)
          listOfMultiEdgeRel.map(body => Def(tmpRelHead, body))
        }).toList // tmp():- L1L3L2(a1, x1, _, ...), L1L3L2(x1, x2, _, ...),L1L3L2(x2, a2, _, ...)
        (Some(List(tmpDecl) ++ tmpDefList), tmpRelHead)
      }
      case Some(TransDistance(m)) => {
        val tcTypedArgsPreWeight = List(Arg(edge.src.vrb.strg, getNodeIdType(edge.src.label)(env)),
          Arg(edge.trg.vrb.strg, getNodeIdType(edge.trg.label)(env)))
        val tcTypedArgsFinal = if (weight) {
          tcTypedArgsPreWeight :+ Arg("w", CIntType)
        } else {
          tcTypedArgsPreWeight
        }
        val tcUntypedArgs = tcTypedArgsFinal.map(e => e.x)
        val tcName = freshName("TC")
        val tcDecl = Decl(tcName, tcTypedArgsFinal) // .decl C(src:type, trg:type, w:number) or TC(src, trg)
        val tcDefHead = Relation(tcName, tcUntypedArgs) // TC(src, trg, w) or TC(src, trg)
        // Get a list of edge idb that has the same edge label as edgeIDB,
        val edgeIDBs = getVarLenPathEdgeIDBs(edgeIDB)(env)
        val transClosureBodyList = generateMultiEdgeRel(m, edge.src.vrb, edge.trg.vrb)(edgeIDBs)(weight)(env)
        // TODO: adapt to list form
        val tcDefBody1: List[Def] = transClosureBodyList.map(body => Def(tcDefHead, body))
        val tcDefBody2: List[Def] = if (weight) {
          edgeIDBs.map(idb => {
            val numFields = numberOfAttr(idb)(env.schema)
            Def(Relation(tcName, tcUntypedArgs), List(
              Relation(tcName, List(edge.src.vrb.strg, "tmp", "w2")),
              Relation(idb, List("tmp", edge.trg.vrb.strg) ++ List.fill(numFields+1)("_")),
              CmpRel("=", DlVar("w"), Arith("+", DlVar("w2"), Constant(1)))))
          })
        } else {
            edgeIDBs.map(idb => {
            val numFields = numberOfAttr(idb)(env.schema)
            Def(Relation(tcName, tcUntypedArgs), List(
              Relation(tcName, List(edge.src.vrb.strg, "tmp")),
              Relation(idb, List("tmp", edge.trg.vrb.strg) ++ List.fill(numFields+1)("_"))
            ))
          })
        } // TR(p, x, w2), edgeIDB(x, q), (w = w2 + 1).
        (Some(List(tcDecl) ++ tcDefBody1 ++ tcDefBody2), tcDefHead)
      }
    }
  }


  def getLabelFromVarInfo(labelspec: LabelSpec)(schema: CyphSchema): String = labelspec match {
    case LabelInput(name) => name
    case l : LabelOr => {
      // Edge case
      val containUnionEdge = l.list.find(input => input.name.contains("_")) match {
        case Some(_) => true
        case None => false
      }
      if (containUnionEdge) {
      l.list.head.name // return either one (this logic could be faulty)
      } else {
        schema.types.collect { case n: NodeType if n.spec.isInstanceOf[LabelOr] => n }.find(n =>
          n.spec.asInstanceOf[LabelOr].list.toSet.equals(l.list.toSet)) match {
          case Some(node) => node.name
          case None => throw new Exception(s"Error: Can not find the parent Label of $labelspec!")
        }
      }
    }
    case other => throw new Exception(s"Can not get Label information for $other")
  }
  def inferVarTypes(patterns:List[Pattern], variable: Var): LabelSpec = {
    // Assumption: patterns is a list of edge patterns
    val varmap = Map[Var, Set[String]]()
    val edgePatterns = patterns.collect{ case e: EdgePattern => e}
    val interimVarMap = edgePatterns.foldLeft(varmap)((accMap, curEdge) => {
      val newMap = if (accMap.contains(curEdge.src.vrb)) {
        val existingLabels = accMap(curEdge.src.vrb)
        val nLabelSet =  existingLabels + curEdge.src.label
        accMap + (curEdge.src.vrb -> nLabelSet)
      } else {
        accMap + (curEdge.src.vrb -> Set(curEdge.src.label))
      }
      if (newMap.contains(curEdge.trg.vrb)) {
        val existingLabels = accMap(curEdge.trg.vrb)
        val newLabelSet = existingLabels + curEdge.trg.label
        newMap + (curEdge.trg.vrb -> newLabelSet)
      } else {
        newMap + (curEdge.trg.vrb -> Set(curEdge.trg.label))
      }
    })
    // Add node pattern case
    val nodePatterns = patterns.collect{ case n: NodePattern => n}
    val finalVarMap = nodePatterns.foldLeft(interimVarMap)((accMap, curNode) => {
      if (accMap.contains(curNode.vrb)){
        val existingLabels = accMap(curNode.vrb)
        val nLabelSet = existingLabels + curNode.label
        accMap + (curNode.vrb -> nLabelSet)
      } else {
        accMap + (curNode.vrb -> Set(curNode.label))
      }
    })

    if (finalVarMap(variable).size == 1) {
      LabelInput(finalVarMap(variable).head)
    } else {
      LabelOr(finalVarMap(variable).toList.map(l => LabelInput(l)))
    }
  }

  def transUnionMatch(patterns: List[Pattern])(weight: Boolean)(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    // Assume: all patterns have the same variables by construction
    // .decl Match(m:unsigned, ...)
    // Match(m,...) :- prevRel, Comment_HAS_...(m, sdf), ...
    // Match(m,...) :- prevRel, Comment_HAS_...(m, sdf), ...
    // pass Match(m,...) to the new environment as prevRel
    // Also, if a variable m is from a previous UNWIND clause, need to add reference relation in the body.
    val prevClauseTypedArgs = env.prevRel.args
    val firstEdgePattern: EdgePattern = patterns.collectFirst {
      case e: EdgePattern => e
    } match {
      case Some(e) => e
      case None => throw new Exception("No Edge Pattern is found in UNION MATCH")
    }
    val currClauseTypedArgsWithoutWeight = getMatchArgsHelper(firstEdgePattern)(env) // TODO: patterns.head, is not correct (Get new arguments for graph pattern)
    // TODO (later): Ignore weight for now
    val newMatchTypedArgs = (prevClauseTypedArgs ++ currClauseTypedArgsWithoutWeight).distinct
    val relName = freshName("Match")
    // .decl Match(...) and Match(...)
    val matchDecl = Decl(relName, newMatchTypedArgs)
    val matchDefHead = Relation(relName, newMatchTypedArgs.map(arg => arg.x))
    // Get list (a relation) reference
    val refRelationList = newMatchTypedArgs.filter(arg => env.unwindVar.contains(arg.x)).map(arg => env.unwindVar(arg.x)) // handel variable created by previous UNWIND clause
    // rename variables in previous relation whenever there is var collision
    val varInCurrClause = currClauseTypedArgsWithoutWeight.map(arg => arg.x)
    val renamed_prev_rel = env.prevRel.rel match {
      case Some(relation) => {
        val fields = relation.x.map(vrb => if (varInCurrClause.contains(vrb) && env.withFilter.contains(Var(vrb))) {
          freshName(vrb)
        } else {
          vrb
        })
        List(Relation(relation.a, fields))
      }
      case None => List()
    }

    // Get bodies
    val bodies = patterns.map {
      case pattern@EdgePattern(src, trg, vrb, _, _, _) => {
        val (rules, relation) = transEdge(pattern, weight)(env)
        val srcNodeRel = transNode(src)(env)
        val trgNodeRel = transNode(trg)(env)
        val body = List(relation, srcNodeRel, trgNodeRel) ++ refRelationList
        val matchDefBody = renamed_prev_rel ++ body
        val extraRelations = rules match {
          case None => List()
          case Some(tmpRules) => tmpRules
        }
        (extraRelations, matchDefBody)
      }
      case pattern@NodePattern(vrb, label) => {
        val singleNodeRel = transNode(pattern)(env)
        val unifyVar = if (vrb == firstEdgePattern.src.vrb) {
          firstEdgePattern.trg.vrb
        } else {firstEdgePattern.src.vrb}
        val body = List(singleNodeRel, CmpRel("=", DlVar(vrb.strg), DlVar(unifyVar.strg))) ++ refRelationList
        val matchDefBody = renamed_prev_rel ++ body
        (List(), matchDefBody)
      }
      case other => throw new Exception(s"$other is not supported in UnionMatch")
    }
    val matchDefs = bodies.map(tup => Def(matchDefHead, tup._2))
    val matchBodyRelationNames = matchDefs.flatMap(de => de.b).collect{case r: Relation if r.a.contains("_") => r.a}
    val extraRels = bodies.map(tup => tup._1).foldLeft[List[Rule]](List())((acc, cur) => acc ++ cur)
    val oneEdgeRelation = patterns.collectFirst{ case pattern: EdgePattern => pattern} match {
      case Some(e) => e
      case None => throw new Exception("Edge Pattern is not found in UNION MATCH")
    }
    val newPrevRel = CypherPrevRel(newMatchTypedArgs, Some(matchDefHead), (env.prevRel.newargs ++ currClauseTypedArgsWithoutWeight).distinct)
    (DatalogQuery((extraRels :+ matchDecl) ++ matchDefs),
      CypherEnv(newPrevRel, env.varInfo +
        (oneEdgeRelation.src.vrb -> CypherVarEnv(inferVarTypes(patterns, oneEdgeRelation.src.vrb), None, None)) +
        (oneEdgeRelation.trg.vrb -> CypherVarEnv(inferVarTypes(patterns, oneEdgeRelation.trg.vrb), None, None)) +
        (oneEdgeRelation.vrb -> CypherVarEnv(LabelOr(matchBodyRelationNames.map(name => LabelInput(name))), Some(oneEdgeRelation.src.vrb), Some(oneEdgeRelation.trg.vrb))),
        env.schema, env.table, env.newRules, env.varTypes ++ newMatchTypedArgs.map(arg => Var(arg.x) -> arg.t), env.withFilter,env.propertyRegistry, env.collectVars, env.unwindVar))
  }

  def transOptionalMatch(clauses: List[Clause])(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    // Only match or match1, match2, .. where (a single where)
    // Separate Match Clause and WHERE clause first
    // TODO: fix var collision!
    val (matchesCl, whereCl) = clauses.last match {
      case Where(e) => {
        val whereClause = clauses.last
        val matchesClause = clauses.dropRight(1)
        (matchesClause, whereClause.asInstanceOf[Where])
      }
      case _ => (clauses, null)
    }

    val (initMatch, initEnv) = {
      val emptyEnv = CypherEnv(CypherPrevRel(List(), None, List()), env.varInfo, env.schema, env.table, env.newRules, env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      matchesCl.head match {
      case c: Match => transMatch(c.p)(weight = false)(emptyEnv)
      case c: UnionMatch => transUnionMatch(c.p)(weight = false)(emptyEnv)
    }}
    // Process Match/UnionMatch clauses
    val (matchEnv, matchRules) = matchesCl.drop(1).foldLeft((initEnv, initMatch.rules))((acc, cur) => {
      val (newMatchRule, updatedEnv) = cur match {
        case c: Match => transMatch(c.p)(weight = false)(acc._1)
        case c: UnionMatch => transUnionMatch(c.p)(weight = false)(acc._1)
      }
      (updatedEnv, acc._2 ++ newMatchRule.rules)
    })
    // Process Where clause if there is
    val (newEnv, matchWithWhereRules) = whereCl match {
      case Where(expr) => {
        val (matchWithWhereDLog, subMatchFinalEnv) = transWhere(expr)(matchEnv)
        (subMatchFinalEnv, matchRules ++ matchWithWhereDLog.rules)
      }
      case null => (matchEnv, matchRules)
    }

    val prevClauseTypedArgs = env.prevRel.args
    val optionMatchTypedArgs = (prevClauseTypedArgs ++ matchEnv.prevRel.args).distinct
    val optionMatchUntypedArgs = optionMatchTypedArgs.map(arg => arg.x)
    val relName = freshName("OpMatch")
    val opMatchDecl = Decl(relName, optionMatchTypedArgs)
    val opMatchDefHead = Relation(relName, optionMatchUntypedArgs)

    val finalSubMatchRel = matchWithWhereRules.last.asInstanceOf[Def].a
    val prevClause = env.prevRel.rel
    val opMatchDefBodyFound = List(prevClause.get, finalSubMatchRel) // match_n(), match_m(), ...
    // New Variables introduced in Optional Match
    val newOpMatchVar = newEnv.prevRel.args.map(key => Var(key.x)).toSet.diff(env.varInfo.keySet).toList
    val nullVarAtoms = newOpMatchVar.map(vrb => CmpRel("=", DlVar(vrb.strg), Constant(genNullValue(newEnv.varTypes(vrb)))))
    val unfoundRelation = Neg(Relation(finalSubMatchRel.a, finalSubMatchRel.x.map(v => if (newOpMatchVar.contains(Var(v))) {
      "_"
    } else {
      v
    })))
    val opMatchDefBodyUnfound = List(prevClause.get, unfoundRelation) ++ nullVarAtoms // match_n(), !match_m(), new_var = null ....

    val dataLogQuery = DatalogQuery(matchWithWhereRules ++ List(opMatchDecl, Def(opMatchDefHead, opMatchDefBodyFound),
      Def(opMatchDefHead, opMatchDefBodyUnfound)))
    val updatedEnv = CypherEnv(CypherPrevRel(optionMatchTypedArgs, Some(opMatchDefHead), (env.prevRel.newargs ++ matchEnv.prevRel.newargs).distinct),
      newEnv.varInfo, env.schema, env.table, env.newRules, newEnv.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
    (dataLogQuery, updatedEnv)
  }

  def transWhere(expr: Expr)(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    val relName = freshName("Where")
    val prevClauseTypedArgs = env.prevRel.args
    val whereDecl = Decl(relName, prevClauseTypedArgs)
    val whereHead = Relation(relName, prevClauseTypedArgs.map(arg => arg.x))

    val (body, newEnv) = transExpr(expr)(env)
    val whereBody = env.prevRel.rel match {
      case Some(rel) => rel :: body
      case None => body
    }
    val whereDef = Def(whereHead, whereBody)
    val dLogQuery = DatalogQuery(newEnv.newRules ++ List(whereDecl, whereDef))
    val newPrevRel = CypherPrevRel(prevClauseTypedArgs, Some(whereHead), (env.prevRel.newargs ++ newEnv.prevRel.newargs).distinct)
    (dLogQuery, CypherEnv(newPrevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes, env.withFilter, Map(), env.collectVars, env.unwindVar)) // reset property registry
  }

  def transWithDistinct(exprAs: List[ExprAs])(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    val (withDistTypedArgs, nenv) = exprAs.foldLeft[(List[Arg], CypherEnv)]((List(), env))((acc, cur) => {
      val (ty, accEnv) = getExprAsTypes(cur)(acc._2)
      (acc._1 :+ Arg(cur.a.strg, ty), accEnv)
    })

    // translate body :- ...
    val (subrules, withDistinctBody) = exprAs.foldLeft[(List[Rule], List[Atom])]((List(), List()))((acc, cur) => {
      val (translatedAtom, newenv) = transExprAs(cur)(nenv)
      val rules = newenv.newRules
      (acc._1 ++ rules, acc._2 ++ translatedAtom)
    })

    val relName = freshName("WithDistinct")
    val withDistDecl = Decl(relName, withDistTypedArgs)

    val withDistUntypedArgs = withDistTypedArgs.map(arg => arg.x)
    val withDistDefHead = Relation(relName, withDistUntypedArgs)

    val withDistDefBody = env.prevRel.rel match {
      case Some(rel) => rel :: withDistinctBody
      case None => withDistinctBody
    }
    val withDistDef = Def(withDistDefHead, withDistDefBody)
    val newPrevRel = CypherPrevRel(withDistTypedArgs, Some(withDistDefHead), withDistTypedArgs)
    (DatalogQuery(subrules ++ List(withDistDecl, withDistDef)),
      CypherEnv(newPrevRel, nenv.varInfo, nenv.schema, nenv.table, nenv.newRules, nenv.varTypes, List(), env.propertyRegistry, env.collectVars, env.unwindVar)) // reset withFilter in env
  }

  /* Rules of translation
  1. does not remove (only add) variables from arguments
  2.
  * */

  def transWith(exprAs: List[ExprAs])(env: CypherEnv): (DatalogQuery, CypherEnv) = {
    val relName = freshName("With")
    // translate arguments types
    val (withNewTypedArgs, nenv) = exprAs.foldLeft[(List[Arg], CypherEnv)]((List(), env))((acc, cur) => {
      val (ty, accEnv) = getExprAsTypes(cur)(acc._2)
      (acc._1 :+ Arg(cur.a.strg, ty), accEnv)
    })
    val prevRelTypedArgs = env.prevRel.args
    val deprecatedVariables = prevRelTypedArgs.filter(arg => !withNewTypedArgs.contains(arg)).map(ar => Var(ar.x))
    /* remove vrb that has appeared in collect(vrb) previously from previous args
    * This is used in case like CQ10:
    * OPTIONAL MATCH (friend:Person)<-[:HAS_CREATOR]-(post:Post)
    * WITH friend, city, collect(post) AS posts, person
    * WITH clause receive post from OPTMATCH clause, but since post is collected in WITH, it
    * should not be included as arg in IDB WITH(friend, city, ...)
     */
    val collectedVars = prevRelTypedArgs.filter(arg => env.table.contains(arg.x)).map(arg => Var(arg.x))
    val withTypedArgsWithoutCaseVar = {
      val varsCoexistWithCollectedVar = collectedVars.flatMap(vrb => env.collectVars(vrb))
      val varsToRemoveFromWith = (varsCoexistWithCollectedVar.diff(withNewTypedArgs.map(arg => Var(arg.x))) ++ collectedVars).distinct
      val allVars = (prevRelTypedArgs ++ withNewTypedArgs).distinct
      allVars.filter(arg => !varsToRemoveFromWith.contains(Var(arg.x)))
    }

    // translate body :- ...
    val (subrules, withSubBody) = exprAs.foldLeft[(List[Rule], List[Atom])]((List(), List()))((acc, cur) => {
      val (rules, atom) = cur match {
        case ExprAs(Case(_, _, _, _), a) => transCaseAs(cur)(nenv) // Special treatment of CASE(...)
        case _ => {
          val (translatedBody, newEnv) = transExprAs(cur)(nenv)
          (newEnv.newRules, translatedBody)
        }
      }
      (acc._1 ++ rules, acc._2 ++ atom)
    })

    /* Get non-duplicate arguments of Case1(arg1:ty1, arg2:ty2, arg3:ty3) and Case2(arg4:ty4, arg5:ty5, arg6:ty6)
    * (arg1:ty1, arg2:ty2, arg3:ty3, arg4:ty4, arg5:ty5, arg6:ty6)
    * Search if any argument type of With is UnknownType, if so, find the type in the case arguments.
    * */
    val allCaseArgs = subrules.collect { case r: Decl => r }.flatMap(_.args).distinct
    val withTypedArgs = withTypedArgsWithoutCaseVar.map(arg => arg.t match {
      case UnknownType => {
        val ty = allCaseArgs.filter(c => c.x == arg.x).map(a => a.t).head
        Arg(arg.x, ty)
      }
      case _ => arg
    })

    // .decl With(prevArgs, newArgs)
    val withDecl = Decl(relName, withTypedArgs)

    // With(prevArgs, newArgs)
    val withUntypedArgs = withTypedArgs.map(arg => arg.x)
    val withDefHead = Relation(relName, withUntypedArgs)

    val withDefBody = env.prevRel.rel match {
      case Some(rel) => rel :: withSubBody
      case None => withSubBody
    }
    val withDef = Def(withDefHead, withDefBody)
    val newPrevRel = CypherPrevRel(withTypedArgs, Some(withDefHead), withNewTypedArgs)
    (DatalogQuery(subrules ++ List(withDecl, withDef)),
      CypherEnv(newPrevRel, nenv.varInfo, nenv.schema, nenv.table, nenv.newRules, nenv.varTypes, nenv.withFilter ++ deprecatedVariables, env.propertyRegistry, Map(), env.unwindVar)) // TODO: revisit this! reset collectedVars
  }

  def transWithCollect(clause: Clause)(env: CypherEnv): (DatalogQuery, CypherEnv) = clause match {
    // TODO: support collect(distinct ...)
    case WithCollect(expr, attribute, relation, groupKeys, unwinded) => {
      // Obtain type
      val (attributeType, nenv) = getExprAsTypes(ExprAs(expr, attribute))(env)
      // Decl
      val withCoTypedArgs = (List(Arg(attribute.strg, attributeType)) ++ groupKeys.map(vrb => Arg(vrb.strg, getExprAsTypes(ExprAs(Var(vrb.strg), Var(vrb.strg)))(nenv)._1)) :+ Arg("mul", CIntType))
      val withCoDecl = Decl(relation.strg, withCoTypedArgs)
      // Def
      val withCoDefHead = Relation(relation.strg, withCoTypedArgs.map(_.x))
      val (subWithColRules, withCoDefTempBody) = {
        val (subrules, relation) = expr match {
          case Case(_, _, _, _) => transCaseAs(ExprAs(expr, attribute))(nenv) // collect( CASE WHEN ... ) AS ...
          case _ => {
            val (translated, newEnv) = transExprAs(ExprAs(expr, attribute))(nenv) // collect ( ... ) AS ...
            (newEnv.newRules, translated)
          }
        }
        val prevClause = nenv.prevRel.rel match {
          case Some(rel) => List(rel)
          case None => List()
        }
        (subrules, prevClause ++ relation)
      }
      // Create a rule for withCoDefBody: Countxxx(attr, groupKeys, mul):- ....
      val coCountTypedArgs = (Arg(attribute.strg, attributeType) +: nenv.prevRel.args).distinct
      val coCountUntypedArgs = coCountTypedArgs.map(e => e.x)
      // TODO: Fix logic here
      val coCountAttrPlusGroupKeysArg = coCountUntypedArgs.head +: coCountUntypedArgs.tail.map(e => {
        if (groupKeys.contains(Var(e))) {
          e
        } else {
          "_"
        }
      })
      val coCountRelation = coCountAttrPlusGroupKeysArg.map(arg => if (arg == "_") {
        freshName("t")
      } else {
        arg
      })
      val coCountName = s"Count${relation.strg}"
      val coCountDecl = Decl(coCountName, coCountTypedArgs)
      val coCountDefHead = Relation(coCountName, coCountUntypedArgs)
      val coCountDefBody = withCoDefTempBody
      val coCountDef = Def(coCountDefHead, coCountDefBody)
      val withCoDefBody = List(
        Relation(coCountName, coCountAttrPlusGroupKeysArg),
        CmpRel("=", DlVar("mul"), Count(List(Relation(coCountName, coCountRelation)))),
        CmpRel("!=", DlVar(attribute.strg), Constant(genNullValue(attributeType)))
      )
      val withCoDef = Def(withCoDefHead, withCoDefBody)
      val dataLogQuery = DatalogQuery(subWithColRules ++ List(coCountDecl, coCountDef) ++ List(withCoDecl, withCoDef))
      // update the group keys associated with relation too
      val updatedTable1 = nenv.table +
        (relation.strg -> (attribute.strg, Some(withCoDefHead)))  // relation name -> (attr_name, Relation)
      // TODO:
      val updatedTable2 = if (!unwinded) {
        updatedTable1 + (attribute.strg -> (relation.strg, Some(withCoDefHead))) // attr_name => (Relation_name, _)
      } else { updatedTable1 }

      val updatedUnwindVar = if (unwinded) {
        nenv.unwindVar + (attribute.strg -> withCoDefHead)
      } else { nenv.unwindVar }

      // add Var() in collect(Var(...)) to updatedTable for later use in narrowing scope in WITH clause
      val updatedTableWithCollectVar = expr match {
         case Var(vrb) => updatedTable2 + (vrb -> (relation.strg, None))
         case _ => updatedTable2
       }
      // update variables that Var() in collect(Var) brought with from previous relation
      val updatedCollectVars = expr match {
        case Var(vrb) => nenv.collectVars + (Var(vrb) -> nenv.prevRel.args.map(arg => Var(arg.x)))
        case _ => nenv.collectVars
      }
      val newEnv = CypherEnv(nenv.prevRel, nenv.varInfo, nenv.schema, updatedTableWithCollectVar, nenv.newRules,
        nenv.varTypes + (Var(attribute.strg) -> attributeType), nenv.withFilter, nenv.propertyRegistry, updatedCollectVars, updatedUnwindVar)
      // Return
      (dataLogQuery, newEnv)
    }
    case _ => throw new Exception("Arg Type Error: clause should be WithCollect type")
  }

  def transCaseAs(caseAs: ExprAs)(env: CypherEnv): (List[Rule], List[Relation]) = caseAs match {
    // caseAs is assumed ExprAs(Case(...), A)
    case ExprAs(Case(expr, whenExpr, thenExpr, elseExpr), rename) => {
      // when()
      val whenTypedArgs = env.prevRel.args
      val whenName = freshName("When")
      val whenDecl = Decl(whenName, whenTypedArgs)
      val whenDefHead = Relation(whenName, whenTypedArgs.map(a => a.x))
      // TODO: fix this ugly part
      // Ad-hoc way of handling IsNULL(pathVar) :( not ideal
      val (translatedWhenExpr, _) = whenExpr match {
        case IsNULL(Var(vrb)) => {
          // Handle shortest path
          if (env.table.contains(vrb)) {
            val spRel = env.table(vrb)._2.get
            // wildcard w field
            val wildcadSPRelField = spRel.x.map {
              case "w" => "_"
              case other => other
            }
            val updatedSPRel = Relation(spRel.a, wildcadSPRelField)
            (List(Neg(updatedSPRel)), env)
          } else {
            transExpr(whenExpr)(env)
          }
        }
        case _ => transExpr(whenExpr)(env)
      }
      // What if we do not include previous relation
      val whenDefBody = env.prevRel.rel match {
        case Some(rel) => rel +: translatedWhenExpr
        case None => translatedWhenExpr
      }
      val whenDef = Def(whenDefHead, whenDefBody)
      // case()
      // Infer then/else expr value type
      val (valueType, _) = getExprAsTypes(ExprAs(thenExpr, Var(freshName("thenLocal"))))(env)
      val caseTypedArgs = env.prevRel.args :+ Arg(rename.strg, valueType)
      val caseName = freshName("Case")
      val caseDecl = Decl(caseName, caseTypedArgs)
      val caseDefHead = Relation(caseName, caseTypedArgs.map(a => a.x))
      val caseDefBodyWhen = (env.prevRel.rel.get +: transExprAs(ExprAs(thenExpr, rename))(env)._1) ++ List(whenDefHead)
      val caseDefBodyElse = (env.prevRel.rel.get +: transExprAs(ExprAs(elseExpr, rename))(env)._1) ++ List(Neg(whenDefHead))
      val caseDefWhen = Def(caseDefHead, caseDefBodyWhen)
      val caseDefElse = Def(caseDefHead, caseDefBodyElse)
      // return
      (List(whenDecl, whenDef, caseDecl, caseDefWhen, caseDefElse), List(caseDefHead))
    }
    case _ => throw new Exception("Argument type is not ExprAs")
  }

  def buildDisjunct(expr: List[Atom]): Atom = expr match {
    case Nil => throw new Exception("Error: Trying to build disjunctin from amn empty list")
    case atom :: Nil => atom
    case first :: rest => Disjunction(List(first), List(buildDisjunct(rest)))
  }

  def transExprAs(eas: ExprAs)(env: CypherEnv): (List[Atom], CypherEnv) = eas.e match {
    case Value(value) => (List(CmpRel("=", DlVar(eas.a.strg), Constant(value))), env)
    case Var(variable) => {
      /* if variable is the attribute of a list , find the list relation
      from the env and return that relation
      otherwise, bind the variable to its new name
       */
      if (env.unwindVar.contains(variable)) {
        val relation = env.unwindVar(variable)
        (List(relation, CmpRel("=", DlVar(variable), DlVar(eas.a.strg))), env)
      } else {
        (List(CmpRel("=", DlVar(variable), DlVar(eas.a.strg))), env)
      }
    }
    case Key(variable, Var("id")) => {
        val varinfo = env.varInfo(variable)
        val relName = lookupIdb(getLabelFromVarInfo(varinfo.label)(env.schema))(env.schema)
        val identifiers = (varinfo.src, varinfo.trg) match {
          case (Some(Var(src)), Some(Var(trg))) => List(src, trg, variable.strg)
          case (None, None) => List(variable.strg)
        }
        val untypedArgs = identifiers ++ List.fill(numberOfAttr(relName)(env.schema))("_")
        val atom1 = Relation(relName, untypedArgs)
        val atom2 = CmpRel("=", DlVar(variable.strg), DlVar(eas.a.strg))
        (List(atom1, atom2), env)
    }
    // TODO: when key is an edge variable that has union labels,
    //  return disjunction of retrieving its properties
    case Key(variable, Var(property)) => {
      val varinfo = env.varInfo(variable)
      //*** fix
      val relName = varinfo.label match {
        case l: LabelOr => l.list.map(spec => spec.name)
        case _ => List(lookupIdb(getLabelFromVarInfo(varinfo.label)(env.schema))(env.schema))
      }

      val identifiers = (varinfo.src, varinfo.trg) match {
        case (Some(Var(src)), Some(Var(trg))) => List(src, trg, variable.strg)
        case (None, None) => List(variable.strg)
      }

      val listAtom = relName.map(rel_name => {
        val untypedArgs = identifiers ++ List.fill(numberOfAttr(rel_name)(env.schema))("_")
        val propertyIdx = indexOfAttrInIDB(rel_name, property)(env.schema)
        val updatedArgs = untypedArgs.updated(propertyIdx, eas.a.strg)
        val atom = Relation(rel_name, updatedArgs)
        atom
      }).toList

      (List(buildDisjunct(listAtom)), env)
    }
    case AggSum(arg, groupKeys) => arg match {
      /* TODO: sum Relation(groupkeys, target of sum, ___ ) wildcard the rest
      so we need information about group keys, do we need this in AggSum or separately?
      makes sense to have it in AggSum but then we need to handel this in lowering, which
      can do. This is easy to do in lowering because in translation you have to identify the
      grouping keys which are more obvious in AST lowering
      */
      case Var(sumVar) => {
        val prevRelation = env.prevRel.rel.get // For example: With(person, x1, x2, sumVar, x3)
        val sumRelation = Relation(prevRelation.a, prevRelation.x.map(vrb =>
          if (vrb == sumVar || groupKeys.contains(Var(vrb))) {
            vrb
          } else {
            "_"
          })) // Wildcard non-group keys => With(person, _, _, sumVar, _)
        val sumAtom = Sum(sumVar, sumRelation)
        (List(CmpRel("=", DlVar(eas.a.strg), sumAtom)), env)
      }
      case unknown => throw new Exception(s"$unknown is not supported for sum()")
    }
    // FIX THIS NOW
    case AggCount(arg, groupKeys) => arg match {
      case Var(countVar) => {
        /* countVar should have been added to varTypes in env */
        val nullValue = genNullValue(env.varTypes(Var(countVar)))
        val relationToCount = env.prevRel.rel.get
        // Rename variables exist in env.newargs, also rename countVar
        val newVarsFromPrevRel = env.prevRel.newargs.map(arg => arg.x)
        val renamedRelToCount = Relation(relationToCount.a,relationToCount.x.map(vrb => {
          if (groupKeys.contains(Var(vrb))) {
            vrb
          } else if (newVarsFromPrevRel.contains(vrb) || vrb == countVar) {
            vrb ++ "1"
          } else {
            vrb
          }
        }))

        val countAtom = Count(List(renamedRelToCount, CmpRel("!=", DlVar(countVar ++ "1"), Constant(nullValue))))
        (List(CmpRel("=", DlVar(eas.a.strg), countAtom)), env)
      }
      case Distinct(expr) => {
        val (subrules, relation) = transDistinct(expr, groupKeys)(env)
        val countVar = expr match {
          case Var(variable) => variable
          case unknown => throw new Exception(s"$unknown is not supported as argument for count()")
        }
        val newCountVar = freshName(countVar)
        val distnctRel = Relation(relation.a, relation.x.map {
          case v if (v == countVar) => newCountVar
          case other => other
        })
        val countAtom = Count(List(distnctRel, CmpRel("!=", DlVar(newCountVar), Constant(genNullValue(env.varTypes(Var(countVar)))))))
        (List(CmpRel("=", DlVar(eas.a.strg), countAtom)),
          CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, subrules, env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
      }
      case unknown => throw new Exception(s"$unknown is not supported for count()")
    }
    case AggSize(arg) => arg match {
      case Var(tableName) => {
        if (!env.table.contains(tableName)) {
          throw new Exception(s"List $tableName is not found in environment")
        }
        val mulName = freshName("mul")
        val sumRelation = {
          val collectRel = env.table(tableName)._2.get
          Relation(collectRel.a, collectRel.x.dropRight(1) :+ mulName)
        }
        val sumAtom = Sum(mulName, sumRelation)
        (List(CmpRel("=", DlVar(eas.a.strg), sumAtom)), env)
      }
      case ListComp(sizeVar, refVar, predicate, groupKeys) => {
        val (listRel, newEnv) = transExprAs(ExprAs(arg, eas.a))(env)
        val sumAtom = Sum("mul", listRel.head)
        (List(CmpRel("=", DlVar(eas.a.strg), sumAtom)), newEnv)
      }
      case unknown => throw new Exception(s"$unknown is not supported for size()")
    }
    case Cmp(_, Var(_), _) => {
      // val (transCmp, _) = transExpr(eas.e)(env)
      // (List(CmpRel("=", DlVar(eas.a.strg), transCmp.head)), env)
      // ???
      val transTerm = transExprToTerm(eas.e)(env)
      (List(CmpRel("=", DlVar(eas.a.strg), transTerm)), env)
    }
    case ListComp(var1, refVar, predicate, groupKeys) => {
      val (inAtoms, _) = transExpr(In(var1, refVar, neg = false))(env)
      val (subPattnRules, predicateRel) = transPatternExpr(predicate)(env)
      // Get variables from graph pattern:
      val varsInGraphPattern = subPattnRules.flatMap(rule => rule match {
        case Decl(a, args) => Some(args)
        case _ => None
      }).flatten
      /* decl for List Match
      * get variable type
      * remove new variables occurred in graph pattern in WHERE
      */
      val newVarInList = Arg(var1.strg, env.varTypes(Var(env.table(refVar.strg)._1)))
      val lstCompTypedArgs = ((newVarInList +: env.prevRel.args)).distinct
      val lstCompName = freshName("ListCmp")
      val lstCompDecl = Decl(lstCompName, lstCompTypedArgs)
      // def
      val lstCompDefHead = Relation(lstCompName, lstCompTypedArgs.map(e => e.x))
      val lstCompDefBody = env.prevRel.rel.get +: (inAtoms :+ predicateRel)
      val lstCompDef = Def(lstCompDefHead, lstCompDefBody)
      // final for matched list
      val newSubRules = subPattnRules ++ List(lstCompDecl, lstCompDef)
      // List with mul field and vars from prev relation
      val groupKeyArgs = env.prevRel.args.flatMap(arg => if (groupKeys.contains(Var(arg.x))) {Some(arg)} else {None})
      val listCompBagTypedArgs = ( groupKeyArgs :+ newVarInList) :+ Arg("mul", CIntType)
      val listCompBagRelName = freshName("ListCmpBag")
      val listCompBagDecl = Decl(listCompBagRelName, listCompBagTypedArgs)
      val listCompBagDefHead = Relation(listCompBagRelName, listCompBagTypedArgs.map(arg => arg.x))
      val onlyGroupKeyLstComp = Relation(lstCompDefHead.a, lstCompDefHead.x.map(attr => if (groupKeys.contains(Var(attr)) || attr == var1.strg) {attr} else {"_"}))
      val listCompBagDefBody = List(onlyGroupKeyLstComp, CmpRel("=", DlVar("mul"), Count(List(lstCompDefHead))))
      val finalSubRules = newSubRules ++ List(listCompBagDecl, Def(listCompBagDefHead, listCompBagDefBody))
      (List(listCompBagDefHead),
        CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, finalSubRules, env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
    }
    case AggMin(arg) => arg match {
      case length: LengthPath => {
        val (relation, nenv) = transExprAs(ExprAs(length, eas.a))(env)
        val minAtom = Min("w", relation.head)
        (List(CmpRel("=", DlVar(eas.a.strg), minAtom)), nenv)
      }
      case other => throw new Exception(s"Use case of min($other) is not supported")
    }
    case LengthPath(Left(pathVar)) => {
      val spRelation = env.table(pathVar.strg)._2.get
      (List(spRelation, CmpRel("=", DlVar("w"), DlVar(eas.a.strg))), env)
    }
    case Not(PatternExpr(pttn, cond)) => {
      /* final output Not(vrb_in_graph_expr, ASvrb)
        * newRules: (update in the env)
        * (1) SUBMATCH(vrb_in_graph_expr):- NODE_EDGE_NODE(....)
        * (2) Not(vrb_in_graph_expr, ASvrb):- PREV_RELATION(...), !SUBMATCH(vrb_in_graph_expr), ASvrb = "true"
        *     Not(vrb_in_graph_expr, ASvrb):- PREV_RELATION(...),  SUBMATCH(vrb_in_graph_expr), ASvrb = "false"
      */
      val (graphMatchSeqRules, graphMatchRel) = transPatternExpr(PatternExpr(pttn, cond))(env)
      val vrb_in_graph_expr = graphMatchSeqRules.collectFirst {
        case d: Decl if d.a == graphMatchRel.a => d.args
      }.get
      val not_args = vrb_in_graph_expr.intersect(env.prevRel.args)
      val not_args_string = not_args.map(e => e.x)
      val not_typed_args =  not_args :+ Arg(eas.a.strg, CStringType)
      val graphMatchRelBoundedVar = graphMatchRel.x.map(s => if (not_args_string.contains(s)) {s} else {"_"})
      val graphMatchRelBounded = Relation(graphMatchRel.a, graphMatchRelBoundedVar)
      val not_rel_name = freshName("Not")
      val not_decl = Decl(not_rel_name, not_typed_args)
      val not_def_head = Relation(not_rel_name, not_typed_args.map(a => a.x))
      val not_defpos = Def(not_def_head, List(env.prevRel.rel.get, Neg(graphMatchRelBounded), CmpRel("=", DlVar(eas.a.strg), Constant("true"))))
      val not_defneg = Def(not_def_head, List(env.prevRel.rel.get, graphMatchRelBounded, CmpRel("=", DlVar(eas.a.strg), Constant("false"))))
      val rules_to_pass_on = graphMatchSeqRules ++ List(not_decl, not_defpos, not_defneg)
      (List(not_def_head), CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, rules_to_pass_on, env.varTypes + (eas.a -> CStringType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
    }
    case unknown => throw new Exception(s"$unknown is not supported in ExprAs translation")
  }

  /*
  * Given a graph expression, output (decl & def created for the graph expression, final relation)
  */
  def transPatternExpr(pattern: Expr)(env: CypherEnv): (List[Rule], Relation) = pattern match {
    case pttn: PatternExpr => {
      val newEnv = CypherEnv(CypherPrevRel(List(), None, List()), Map(), env.schema, Map(), List(), env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      // translate graph pattern
      val (pattnRules, resultEnv) = pttn.patterns.foldLeft[(List[Rule], CypherEnv)]((List(), newEnv))((acc, cur) => {
        val (dLogQuery, updatedEnv) = transMatch(cur)(weight = false)(acc._2)
        (acc._1 ++ dLogQuery.rules, updatedEnv)
      })
      // translate condition/constraints
      val (finalRules, finalEnv) = pttn.property match {
        case Some(expr) => {
          val (exprDLogQuery, resultEnv2) = transWhere(expr)(resultEnv)
          (pattnRules ++ exprDLogQuery.rules, resultEnv2)
        }
        case None => (pattnRules, resultEnv)
      }
      (finalRules, finalEnv.prevRel.rel.get)
    }
    case other => throw new Exception(s"$other is not supported in pattern expression translation")
  }

  def transDistinct(expr: Expr, groupKeys: List[Var])(env: CypherEnv): (List[Rule], Relation) = expr match {
    case Var(variable) => {
      // Get groupKeys type info
      val groupKeysTypedArgs = groupKeys.map(vrb => Arg(vrb.strg, env.varTypes(vrb)))
      val distncTypedArgs = List(Arg(variable, env.varTypes(Var(variable)))) ++ groupKeysTypedArgs
      val distncDeclName = s"Distinct${variable}"
      val distncDecl = Decl(distncDeclName, distncTypedArgs)
      val distncDefHead = Relation(distncDeclName, distncTypedArgs.map(e => e.x))
      val distncDefBody = List(env.prevRel.rel.get)
      val distncDef = Def(distncDefHead, distncDefBody)
      (List(distncDecl, distncDef), distncDefHead)
    }
    case unknown => throw new Exception(s"$unknown is not supported for Distinct translation")
  }

  private def transExprToTerm(expr: Expr)(env: CypherEnv): Term = expr match {
    case Cmp(op, e1, e2) if List("-", "+", "*", "/").contains(op) =>
      Arith(op, transExprToTerm(e1)(env), transExprToTerm(e2)(env))
    case Var(v) => DlVar(v)
    case _ =>
      throw new Exception(s"translate term `$expr`")
  }

  private def transExpr(expr: Expr)(env: CypherEnv): (List[Atom], CypherEnv) = expr match {
    case Binop("OR", expr1, expr2) => {
      val (expr1Rule, nenv1) = transExpr(expr1)(env)
      val newRules1 = nenv1.newRules
      val (expr2Rule, nenv2) = transExpr(expr2)(nenv1)
      val newRules = newRules1 ++ nenv2.newRules
      (List(Disjunction(expr1Rule, expr2Rule)), CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, newRules, env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
    }
    case Binop("AND", expr1, expr2) => {
      val (expr1Rule, nenv1) = transExpr(expr1)(env)
//      val newRules1 = nenv1.newRules
      val (expr2Rule, nenv2) = transExpr(expr2)(nenv1)
      val newRules = nenv2.newRules
      (List(expr1Rule, expr2Rule).flatten, CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, newRules, env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
    }
    case Cmp(op, Key(variable, property), someValue) => {
      val varinfo = env.varInfo(variable)
      val relName = lookupIdb(getLabelFromVarInfo(varinfo.label)(env.schema))(env.schema)
      val identifiers = (varinfo.src, varinfo.trg) match {
        case (Some(Var(src)), Some(Var(trg))) => List(src, trg, variable.strg)
        case (None, None) => List(variable.strg)
      }
      val untypedArgs = identifiers ++ List.fill(numberOfAttr(relName)(env.schema))("_")

      val value = someValue match {
        case Value(v) => Constant(v)
        case Var(vrb) => DlVar(vrb)
      }

      property match {
        case Var("id") => {
          val atom1 = Relation(relName, untypedArgs)
          val atom2 = CmpRel(op, DlVar(variable.strg), value)
          (List(atom1, atom2), env)
        }
        case Var(prop) => {
          val propertyIdx = indexOfAttrInIDB(relName, prop)(env.schema)
          val freshPropertyName = if (env.propertyRegistry.contains(property) && env.propertyRegistry(property).strg != variable.strg) {
            freshName(property.strg)
          } else {property.strg}
          val updatedArgs = untypedArgs.updated(propertyIdx, freshPropertyName)
          val atom1 = Relation(relName, updatedArgs)
          val atom2 = CmpRel(op, DlVar(freshPropertyName), value)
          val updatePropertyRegEnv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes, env.withFilter,
            env.propertyRegistry + (Var(freshPropertyName) -> variable), env.collectVars, env.unwindVar) // PropertyName -> Key!! easier to search
          (List(atom1, atom2), updatePropertyRegEnv)
        }
      }
    }
    case Cmp(op, Var(vrb1), Var(vrb2)) => {
      (List(CmpRel(op, DlVar(vrb1), DlVar(vrb2))), env)
    }
    case Cmp(op, Var(vrb1), Value(v)) => {
      (List(CmpRel(op, DlVar(vrb1), Constant(v))), env)
    }
    case Cmp(op, Var(vrb1), cmp) => {
      val transTerm = transExprToTerm(cmp)(env)
      (List(CmpRel(op, DlVar(vrb1), transTerm)), env)
    }
    case In(e, table, negation) => {
      if (env.table.contains(table.strg)) {
        val tableRelation = env.table(table.strg)._2.get
        val tableAttr = env.table(table.strg)._1
        val boundedVars = env.prevRel.args.map(arg => arg.x) :+ tableAttr
        // remove unbounded vars from relation fields
        val boundedTableRel = Relation(tableRelation.a, tableRelation.x.map(attr =>
          if (boundedVars.contains(attr)) {attr} else { "_" } ))
        val tableRel = if (negation) {
          Neg(boundedTableRel)
        } else {
          boundedTableRel
        }
        (transExprAs(ExprAs(e, Var(tableAttr)))(env)._1 :+ tableRel, env)
      } else {
        throw new Exception(s"$table is not found in Cypher environment")
      }
    }
    case Not(expr) => expr match {
      case pattn: PatternExpr => {
        val (newSubRules, relation) = transPatternExpr(expr)(env)
        // TODO: wildcard unbounded variables
        // if variables not seen in previous relation, then wildcard
        val varsInPrevRel = env.prevRel.args.map(arg => arg.x)
        val boundedRel = Relation(relation.a, relation.x.map(attr => if (varsInPrevRel.contains(attr)) {attr} else {"_"}))
        (List(Neg(boundedRel)), CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, newSubRules, env.varTypes, env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar))
      }
      case other => throw new Exception(s"$other is not supported in Not ... translation")
    }
    case IsNULL(expr) => expr match {
      case Var(vrb) => {
        /* The case of path variable is handled elsewhere */
        val nullValue = genNullValue(env.varTypes(Var(vrb)))
        (List(CmpRel("=", DlVar(vrb), Constant(nullValue))), env)
      }
      case other => throw new Exception(s"Error: $other expression is not supported for IS NULL")
    }
    case unknown => throw new Exception(s"$unknown is not supported in Expr translation")
  }

  def lookupIdb(label: String)(schema: CyphSchema): String = {
    // Edge type contains "_", node type does not
    if (label.contains("_")) {
      val firstUnderScoreIndex = label.indexOf("_")
      val lastUnderScoreIndex = label.lastIndexOf("_")
      val srcLabel = label.substring(0, firstUnderScoreIndex)
      val destLabel = label.substring(lastUnderScoreIndex + 1)
      val edgeLabel = label.substring(firstUnderScoreIndex + 1, lastUnderScoreIndex)
      schema.types.collectFirst { case e: EdgeType
        if ((e.src.name == srcLabel) && (e.dest.name == destLabel) && (e.rel.name == edgeLabel)) => e
      } match {
        case Some(_) => label
        case None => throw new Exception(s"Error: Edge label $label is not found in the schema")
      }
    } else {
      schema.types.collectFirst { case n: NodeType if (n.name == label) => n } match {
        case Some(_) => label
        case None => throw new Exception(s"Error: Node label $label is not found in the schema")
      }
    }
  }

  private def indexOfAttrInIDB(idb: String, attrName: String)(schema: CyphSchema): Int = {
    if (idb.contains("_")) {
      val firstUnderScoreIndex = idb.indexOf("_")
      val lastUnderScoreIndex = idb.lastIndexOf("_")
      val srcLabel = idb.substring(0, firstUnderScoreIndex)
      val destLabel = idb.substring(lastUnderScoreIndex + 1)
      val edgeLabel = idb.substring(firstUnderScoreIndex + 1, lastUnderScoreIndex)
      val attrIndex = schema.types.collectFirst { case e: EdgeType
        if ((e.src.name == srcLabel) && (e.dest.name == destLabel) && (e.rel.name == edgeLabel)) => e
      } match {
        case Some(e) => e.fields.map(x => x._1).indexOf(attrName) + 2
        case None => throw new Exception(s"Error: $attrName is not found in Edge $idb schema")
      }
      attrIndex
    } else {
      schema.types.collectFirst { case n: NodeType if (n.name == idb) => n } match {
        case Some(n) => n.fields.map(e => e._1).indexOf(attrName)
        case None => throw new Exception(s"Error: $attrName is not found in Node $idb schema")
      }
    }
  }

  private def numberOfAttr(idb: String)(schema: CyphSchema): Int = {
    if (idb.contains("_")) {
      val firstUnderScoreIndex = idb.indexOf("_")
      val lastUnderScoreIndex = idb.lastIndexOf("_")
      val srcLabel = idb.substring(0, firstUnderScoreIndex)
      val destLabel = idb.substring(lastUnderScoreIndex + 1)
      val edgeLabel = idb.substring(firstUnderScoreIndex + 1, lastUnderScoreIndex)
      schema.types.collectFirst { case e: EdgeType if ((e.src.name == srcLabel) && (e.dest.name == destLabel) && (e.rel.name == edgeLabel))
      => e
      } match {
        case Some(e) => e.fields.size - 1
        case None => throw new Exception(s"Error: Edge label $idb is not found in the schema")
      }
    }
    else {
      schema.types.collectFirst { case n: NodeType if (n.name == idb) => n } match {
        case Some(n) => n.fields.size - 1
        case None => throw new Exception(s"Error: Node label $idb is not found in the schema")
      }
    }
  }

  private def getVarLenPathEdgeIDBs(edgeIDB: String)(env: CypherEnv): List[String] = {
    /* Given an edge IDB, for example Comment_ReplyOf_Post, return a list of edge IDB
    that satisfy Comment_ReplyOf_???
     */
    val firstUnderScoreIndex = edgeIDB.indexOf("_")
    val lastUnderScoreIndex = edgeIDB.lastIndexOf("_")
    val srcLabel = edgeIDB.substring(0, firstUnderScoreIndex)
    val edgeLabel = edgeIDB.substring(firstUnderScoreIndex + 1, lastUnderScoreIndex)
    val output = env.schema.types.collect{
      case e: EdgeType if (e.src.name == srcLabel) && (e.rel.name == edgeLabel) => e.src.name + "_" + e.rel.name + "_" + e.dest.name
    }.toList
    output
  }

  private def generateMultiEdgeRel(n: Int, src: Var, trg: Var)(label: List[String])(weight: Boolean)(env: CypherEnv): List[List[Atom]] = {
    val headList = List(src.strg)
    val tailList = List(trg.strg)
    val xList = Range(1, n).foldLeft(List.empty[String])((acc, i) => acc :+ s"x$i")
    val varSrcTrg = (headList ++ xList ++ tailList).sliding(2).toList
    val output = label.map(idb => {
      val numAttr = numberOfAttr(idb)(env.schema)
      varSrcTrg.map(varList => Relation(idb, varList ++ List.fill(numAttr + 1)("_")))
    })
    if (weight) {
      output.map(lst => lst :+ CmpRel("=", DlVar("w"), Constant(n)))
    } else {
      output
    }
  }

  def genNullValue(ty: CyphType): Any = ty match {
    case CLongIntType => -1
    case CIntType => -1
    case CStringType => "NULL"
    case UnknownType => "UndefinedNull"
  }

  def getExprAsTypes(eas: ExprAs)(env: CypherEnv): (CyphType, CypherEnv) = eas.e match {
    // value AS A does not involve renaming, so no update to env
    case Value(v) => v match {
      case l: Long => (CLongIntType, env)
      case n: Int => (CLongIntType, env)
      case s: String => (CStringType, env)
    }
    case Key(variable, Var(property)) => {
      val ty = getPropertyType(variable, property)(env)
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> ty), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (ty, nenv)
    }
    case Var(variable) => {
      // Look up types
      val ty = env.varTypes(Var(variable))
      (ty, env)
    }
    case Cmp(op, e1, e2) => e1 match {
      case v1: Var => {
        val ty = env.varTypes(v1)
        (ty, env)
      }
      case _ => e2 match {
        case v2: Var => {
          val ty = env.varTypes(v2)
          (ty, env)
        }
        case other => throw new Exception(s"Type of $other can not be inferred")
      }

    }
    case AggSum(_, _) => {
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> CLongIntType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (CLongIntType, nenv)
    }
    case AggCount(_, _) => {
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> CIntType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (CIntType, nenv)
    }
    case AggSize(_) => {
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> CIntType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (CIntType, nenv)
    }
    case AggMin(_) => {
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> CIntType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (CIntType, nenv)
    }
    case LengthPath(_) => {
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> CLongIntType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (CLongIntType, nenv)
    }
    case Not(_) => {
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> CStringType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (CStringType, nenv)
    }
    case Case(expr, whenExpr, thenExpr, elseExpr) => {
      val (valueType, _) = getExprAsTypes(ExprAs(thenExpr, Var(freshName("thenLocal"))))(env)
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> valueType), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (valueType, nenv)
    }
    case _ => {
      val ty = UnknownType
      val nenv = CypherEnv(env.prevRel, env.varInfo, env.schema, env.table, env.newRules, env.varTypes + (eas.a -> ty), env.withFilter, env.propertyRegistry, env.collectVars, env.unwindVar)
      (ty, nenv)
    }
  }

  def getPropertyType(vrb: Var, property: String)(env: CypherEnv): CyphType = {
    val schema = env.schema
    val label = getLabelFromVarInfo(env.varInfo(vrb).label)(schema)
    if (label.contains("_")) {
      val firstUnderScoreIndex = label.indexOf("_")
      val lastUnderScoreIndex = label.lastIndexOf("_")
      val srcLabel = label.substring(0, firstUnderScoreIndex)
      val destLabel = label.substring(lastUnderScoreIndex + 1)
      val edgeLabel = label.substring(firstUnderScoreIndex + 1, lastUnderScoreIndex)
      schema.types.collectFirst { case e: EdgeType
        if ((e.src.name == srcLabel) && (e.dest.name == destLabel) && (e.rel.name == edgeLabel)) => e
      } match {
        case Some(e) => e.fields.find(t => t._1 == property) match {
          case Some(tuple) => tuple._2
          case None => throw new Exception(s"$property is not found for $label")
        }
        case None => throw new Exception(s"Error: $property is not found in Edge $label schema")
      }
    } else {
      schema.types.collectFirst { case n: NodeType if (n.name == label) => n } match {
        case Some(n) => n.fields.find(t => t._1 == property) match {
          case Some(tuple) => tuple._2
          case None => throw new Exception(s"$property is not found for $label")
        }
        case None => throw new Exception(s"Error: $property is not found in Node $label schema")
      }
    }
  }

  var globalMatchId = 0
  var globalWhereId = 0
  var globalWithId = 0
  var globalOpMatchId = 0
  var globalWhenId = 0
  var globalCaseId = 0
  var globalListId = 0
  var globalSPId = 0
  var globalNotId = 0
  var globalWithDistId = 0
  var globalGeneralId = 0

  def freshName(name: String): String = name match {
    case "Match" => {
      globalMatchId += 1
      s"Match${globalMatchId}"
    }
    case "Where" => {
      globalWhereId += 1
      s"Where${globalWhereId}"
    }
    case "With" => {
      globalWithId += 1
      s"With${globalWithId}"
    }
    case "WithDistinct" => {
      globalWithDistId += 1
      s"WithDistinct${globalWithDistId}"
    }
    case "OpMatch" => {
      globalOpMatchId += 1
      s"OpMatch${globalOpMatchId}"
    }
    case "When" => {
      globalWhenId += 1
      s"When${globalWhenId}"
    }
    case "Case" => {
      globalCaseId += 1
      s"Case${globalCaseId}"
    }
    case "List" => {
      globalListId += 1
      s"List${globalListId}"
    }
    case "ShortestPath" => {
      globalSPId += 1
      s"ShortestPath${globalSPId}"
    }
    case "Not" => {
      globalNotId += 1
      s"Not${globalNotId}"
    }
    case variable: String => {
      globalGeneralId += 1
      s"$variable$globalGeneralId"
    }
  }

  def resetGlobalId(): Unit = {
    globalMatchId = 0
    globalWhereId = 0
    globalWithId = 0
    globalOpMatchId = 0
    globalWhenId = 0
    globalCaseId = 0
    globalListId = 0
    globalSPId = 0
    globalNotId = 0
    globalWithDistId = 0
    globalGeneralId = 0
  }


}