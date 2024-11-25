package uk.ac.ed.dal
package raqlet
package transformer

import ir._

import SqlogRewrite.groupRules

object DatalogOptimiser {
  /** only store converted Def (idbOptIR) in the output query
   * can be more concise, refactor later!
   * */
  def convertToOptIR_and_build_IDBTable(query: DatalogQuery): (datalogQueryOptIR, Map[String, RelationInfo]) = {
    val groupedIdbs = groupRules(query.rules)
    val transformed_idb_ir_and_relation_info: List[(idbOptIR, (String, RelationInfo))] = groupedIdbs.map {
      case group@((decl: Decl) :: defs) => {
        val original_defs = defs.asInstanceOf[List[Def]]
        val transformed_defs = _transformDef(original_defs)
        decl.a match {
          case name if name.startsWith("NonTC") => (ntClosureDefOptIR(transformed_defs),
            (decl.a -> RelationInfo(relationTypeEnum.nontcIDB, defs.size, decl, Some(original_defs))))
          case name if name.startsWith("TC") => (tClosureDefOptIR(transformed_defs),
            (decl.a -> RelationInfo(relationTypeEnum.tcIDB, defs.size, decl, Some(original_defs))))
          case _ if group.size > 2 => (unionNormDefOptIR(transformed_defs),
            (decl.a -> RelationInfo(relationTypeEnum.unionIDB, defs.size, decl, Some(original_defs))))
          case _ => (transformed_defs.head,
            (decl.a -> RelationInfo(relationTypeEnum.singleIDB, 1, decl, Some(original_defs))))
        }
      }
    }
    // Extract List[idbOptIR]
    val transformed_idb_ir: List[idbOptIR] = transformed_idb_ir_and_relation_info.map(_._1)
    // Extract Map[String, RelationInfo]
    val idb_table: Map[String, RelationInfo] = transformed_idb_ir_and_relation_info.map(_._2).toMap

    (datalogQueryOptIR(transformed_idb_ir), idb_table)
  }

  def update_idb_relation_table(query: datalogQueryOptIR)(env: optimiserEnv): Map[String, RelationInfo] = {
    val old_relation_table = env.relationTable
    query.rules.map {
      case s: singleNormDefOptIR => {
        val old_info = old_relation_table(s.a.a)
        (s.a.a -> RelationInfo(relationTypeEnum.singleIDB, 1, old_info.decl, Some(List(Def(s.a, s.b)))))
      }
      case u: unionNormDefOptIR => {
        val name = get_rule_name(u)
        val old_info = old_relation_table(name)
        val new_def = u.branches.map(b => Def(b.a, b.b))
        (name -> RelationInfo(relationTypeEnum.unionIDB, new_def.size, old_info.decl, Some(new_def)))
      }
      case nontc: ntClosureDefOptIR => {
        val name = get_rule_name(nontc)
        val old_info = old_relation_table(name)
        val new_def = nontc.branches.map(b => Def(b.a, b.b))
        (name -> RelationInfo(relationTypeEnum.nontcIDB, new_def.size, old_info.decl, Some(new_def)))
      }
      case tc: tClosureDefOptIR => {
        val name = get_rule_name(tc)
        val old_info = old_relation_table(name)
        val new_def = tc.branches.map(b => Def(b.a, b.b))
        (name -> RelationInfo(relationTypeEnum.tcIDB, new_def.size, old_info.decl, Some(new_def)))
      }
    }.toMap
  }


  def _transformDef(defs: List[Def]): List[singleNormDefOptIR] = {
    defs.map(rule => {
      singleNormDefOptIR(rule.a, rule.b)
    })
  }

  def _buildEDBTable(schema: DatalogSchema): Map[String, RelationInfo] = {
    val grouped_schema = groupSchema(schema.schema)
    grouped_schema.foldLeft(Map[String, RelationInfo]()) {
      (acc, cur) => {
        val relDecl = cur.head.asInstanceOf[Decl]
        val relationType = relDecl.a.contains("_") match {
          case true => if (cur.size == 1) relationTypeEnum.singleEdgeEDB else relationTypeEnum.unionEdgeEDB
          case false => if (cur.size == 1) relationTypeEnum.singleNodeEDB else relationTypeEnum.unionNodeEDB
        }
        val defsOpt = if (cur.size > 1) Some(cur.tail.asInstanceOf[List[Def]]) else None
        val relInfo = relDecl.a -> RelationInfo(relationType, cur.size, relDecl, defsOpt)
        acc + relInfo
      }
    }
  }

  /** Groups consecutive Decl and Def items into sublists.
   * Each sublist should start with a Decl and include
   * subsequent Def items until the next Decl is encountered.
   */
  def groupSchema(list: List[Either[Rule, Input]]): List[List[Rule]] = {
    val filteredList = list.collect { case Left(rule) => rule } // Step 1: Filter out Inputs
    // Step 2: Group consecutive elements
    filteredList.foldLeft(List[List[Rule]]()) {
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

  def convertToDatalog(opt_query: datalogQueryOptIR)(env: optimiserEnv): DatalogQuery = {
    val relation_table = env.relationTable

    // helper functions
    def _convertSingleNormDef(rule: singleNormDefOptIR): Def = {
      Def(rule.a, rule.b)
    }

    def _get_decl(name: String, relation_table: Map[String, RelationInfo]): Decl = {
      relation_table.get(name) match {
        case Some(info) => info.decl
        case None => throw new Exception(s"Error: Relation $name is not found in relation table")
      }
    }

    val converted_rules: List[Rule] = opt_query.rules.flatMap {
      case s: singleNormDefOptIR => {
        val decl = _get_decl(s.a.a, relation_table)
        val rule = _convertSingleNormDef(s)
        List(decl, rule)
      }
      case u: unionNormDefOptIR => {
        val decl = _get_decl(u.branches.head.a.a, relation_table)
        val rules = u.branches.map(rule => _convertSingleNormDef(rule))
        decl +: rules
      }
      case nt: ntClosureDefOptIR => {
        val decl = _get_decl(nt.branches.head.a.a, relation_table)
        val rules = nt.branches.map(rule => _convertSingleNormDef(rule))
        decl +: rules
      }
      case tc: tClosureDefOptIR => {
        val decl = _get_decl(tc.branches.head.a.a, relation_table)
        val rules = tc.branches.map(rule => _convertSingleNormDef(rule))
        decl +: rules
      }
    }
    DatalogQuery(converted_rules)
  }


  def optimise(query: DatalogQuery)(schema: DatalogSchema): DatalogQuery = {
    // preparation
    val edb_table = _buildEDBTable(schema)
    val (query_opt_ir, idb_table) = convertToOptIR_and_build_IDBTable(query)
    val relation_table = edb_table ++ idb_table
    val nenv = optimiserEnv(relation_table, Set[String](), Set[String](), Map())
    // optimisation
    val query_optimsed = _optimise(query_opt_ir)(nenv)
    // convert to normal Datalog
    val query_opt_datalog = convertToDatalog(query_optimsed)(nenv)
    val final_query = DatalogRewrite.rewrite(query_opt_datalog)
    final_query
  }

  def _optimise(query: datalogQueryOptIR)(env: optimiserEnv): datalogQueryOptIR = {
    val unified_query = unifyExplicitSelfJoin(query)(env)
    val (inlined_query, nenv) = inlineWithIncrementalElimination(unified_query)(env)
    val cleaned_inlined_query = dead_relation_elimination(inlined_query.rules)(nenv)
    val (pushedQuery, _) = pushFilterToNonTCRule(cleaned_inlined_query)(nenv)
    val (finalOptRule, _) = pushFilterToTCRule(pushedQuery)(nenv)
    finalOptRule
  }

  def pushFilterToNonTCRule(query: datalogQueryOptIR)(env: optimiserEnv): (datalogQueryOptIR, optimiserEnv) = {
    if (existNonTCRule(query)) {
      val nonTcRule = getNonTCRule(query)
      val nonTcRuleName = get_rule_name(nonTcRule)
      val nonTcRelation = nonTcRule.branches.head.a
      val rulesContainNonTC = getRulesContainsGivenRelation(query, nonTcRuleName)
      val filterCondition = extractAllNonTCOrTCFilterCondition(rulesContainNonTC, nonTcRelation)
      if (filterCondition.isEmpty){
        (query, env)
      } else {
        val newNonTCRule = ntClosureDefOptIR(nonTcRule.branches.map(b_rule => singleNormDefOptIR(b_rule.a, b_rule.b ++ filterCondition)))
        val newRuleMap = rulesContainNonTC.map {
          case s_rule: singleNormDefOptIR => (s_rule.a.a, singleNormDefOptIR(s_rule.a, s_rule.b.diff(filterCondition)))
          case u_rule: unionNormDefOptIR => {
            val newBranches = u_rule.branches.map(b_rule => {
              if (containsGivenRelation(b_rule, nonTcRuleName)){
                singleNormDefOptIR(b_rule.a, b_rule.b.diff(filterCondition))
              } else {
                b_rule
              }
            })
            val ruleHeadName = u_rule.branches.head.a.a
            (ruleHeadName, unionNormDefOptIR(newBranches))
          }
          case other => (get_rule_name(other), other)
        }.toMap
        val finalRuleMap = newRuleMap + (newNonTCRule.branches.head.a.a -> newNonTCRule)
        updateQueryWithNewRule(query, finalRuleMap)(env)
      }
    } else {
      (query, env)
    }
  }

  def pushFilterToTCRule(query: datalogQueryOptIR)(env: optimiserEnv): (datalogQueryOptIR, optimiserEnv) = {
    if (existTCRule(query)) {
      val TcRule = getTCRule(query)
      val TcRuleName = get_rule_name(TcRule)
      val TcRelation = TcRule.branches.head.a
      val rulesContainTC = getRulesContainsGivenRelation(query, TcRuleName)
      val filterCondition = extractAllNonTCOrTCFilterCondition(rulesContainTC, TcRelation)
      if (filterCondition.isEmpty){
        (query, env)
      } else {
        val newTCRule = ntClosureDefOptIR(TcRule.branches.map(b_rule => {
          if (containsGivenRelation(b_rule,TcRuleName)) {
            singleNormDefOptIR(b_rule.a, b_rule.b)
          } else {
            singleNormDefOptIR(b_rule.a, b_rule.b ++ filterCondition)
          }
        }))
        val newRuleMap = rulesContainTC.map {
          case s_rule: singleNormDefOptIR => (s_rule.a.a, singleNormDefOptIR(s_rule.a, s_rule.b.diff(filterCondition)))
          case u_rule: unionNormDefOptIR => {
            val newBranches = u_rule.branches.map(b_rule => {
              if (containsGivenRelation(b_rule, TcRuleName)){
                singleNormDefOptIR(b_rule.a, b_rule.b.diff(filterCondition))
              } else {
                b_rule
              }
            })
            val ruleHeadName = u_rule.branches.head.a.a
            (ruleHeadName, unionNormDefOptIR(newBranches))
          }
          case other => (get_rule_name(other), other)
        }.toMap
        val finalRuleMap = newRuleMap + (newTCRule.branches.head.a.a -> newTCRule)
        updateQueryWithNewRule(query, finalRuleMap)(env)
      }
    } else {
      (query, env)
    }
  }

  def updateQueryWithNewRule(query: datalogQueryOptIR, ruleMap: Map[String, idbOptIR])
                            (env: optimiserEnv): (datalogQueryOptIR, optimiserEnv) = {
    val updatedRules = query.rules.map(each_rule => {
      val ruleName = get_rule_name(each_rule)
      ruleMap.getOrElse(ruleName, each_rule)
    })
    // todo: update env with new rule def
    (datalogQueryOptIR(updatedRules), env)
  }

  /** Return true if an NonTC rule exists in a query otherwise false
   * */
  def existNonTCRule(query: datalogQueryOptIR): Boolean = {
    query.rules.exists(_.isInstanceOf[ntClosureDefOptIR])
  }


  /** Return true if an TC rule exists in a query otherwise false
   * */
  def existTCRule(query: datalogQueryOptIR): Boolean = {
    query.rules.exists(_.isInstanceOf[tClosureDefOptIR])
  }

  /** Assume only one NonTc exists in a program, get the current definition of that rule
   * */
  def getNonTCRule(query: datalogQueryOptIR): ntClosureDefOptIR = {
    query.rules.collectFirst{
      case nontcrule: ntClosureDefOptIR => nontcrule
    } match {
      case Some(rule) => rule
      case None => throw new Exception("Error: No NonTC rule found in the current query")
    }
  }

  /** Assume only one NonTc exists in a program, get the current definition of that rule
   * */
  def getTCRule(query: datalogQueryOptIR): tClosureDefOptIR = {
    query.rules.collectFirst{
      case tcrule: tClosureDefOptIR => tcrule
    } match {
      case Some(rule) => rule
      case None => throw new Exception("Error: No NonTC rule found in the current query")
    }
  }

  /**
   *  Search through single and union rules and found all rules that use relation of name@ruleName in their
   *  bodies.
   */
  def getRulesContainsGivenRelation(query: datalogQueryOptIR, ruleName: String): List[idbOptIR] = {
    query.rules.collect{
      case rule if containsGivenRelation(rule, ruleName) => rule
    }
  }

  /**
   * Check if a Datalog rule contains a given relation with given name in its body
   */
  def containsGivenRelation(rule: idbOptIR, ruleName: String): Boolean = rule match {
    case s_rule: singleNormDefOptIR => {
      s_rule.b.exists{
        case Relation(name, _) if name == ruleName => true
        case _ => false
      }
    }
    case u_rule: unionNormDefOptIR => {
      u_rule.branches.exists(_.b.exists {
        case Relation(name, _) => name == ruleName
        case _ => false
      })
    }
    case nontc_rule: ntClosureDefOptIR => false
    case tc_rule: tClosureDefOptIR => false
  }

  def extractAllNonTCOrTCFilterCondition(rules: List[idbOptIR], nonTcRelation: Relation): List[Atom] = {
    val filtersFromEachBody: List[List[Atom]] = List()
    val allValidFiltersInRules = rules.foldLeft(filtersFromEachBody) {
      (acc_list, cur_rule) => cur_rule match {
        case s_rule: singleNormDefOptIR => {
          extractNonTCOrTCFilterCondition(s_rule, nonTcRelation) match {
            case Some(conditions) => acc_list :+ conditions
            case None => acc_list
          }
        }
        case u_rule: unionNormDefOptIR => {
          val list_of_filters = u_rule.branches.flatMap(b_rule => extractNonTCOrTCFilterCondition(b_rule, nonTcRelation))
          acc_list ++ list_of_filters
        }
        case nontc_rule: ntClosureDefOptIR => acc_list // do nothing
        case tc_rule: tClosureDefOptIR => acc_list // do nothing
      }
    }
    allValidFiltersInRules.reduce((filter_a, filter_b) => filter_a.intersect(filter_b))
  }

  def extractNonTCOrTCFilterCondition(rule: singleNormDefOptIR, nonTcOrTCRelation: Relation) : Option[List[Atom]] = {
    val varsOfNonTC = nonTcOrTCRelation.x
    if (containsGivenRelation(rule, nonTcOrTCRelation.a)) {
      val result = rule.b.collect{
        case cmp: CmpRel if isValidNonTCOrTCFilter(cmp, varsOfNonTC) => cmp
      }
      Some(result)
    } else {
      None
    }
  }

  def isValidNonTCOrTCFilter(cmprel: CmpRel, varsOfNonTC: List[String]): Boolean = cmprel match {
    case CmpRel(_, DlVar(op1), Constant(op2: Int)) if varsOfNonTC.contains(op1) => true
    case CmpRel(_, DlVar(op1), Constant(op2: String)) if varsOfNonTC.contains(op1) => true
    case CmpRel(_, DlVar(op1), Constant(op2: Long)) if varsOfNonTC.contains(op1) => true
    case CmpRel(_, DlVar(op1), DlVar(op2)) if varsOfNonTC.contains(op1) && varsOfNonTC.contains(op2) => true
    case _ => false
  }

  /** Inline starting from the first rule, eliminate contradictory join on the go
   * */
  def inlineWithIncrementalElimination(query: datalogQueryOptIR)(env: optimiserEnv): (datalogQueryOptIR, optimiserEnv) = {
    // obtain relation not to inline and add to env
    val nonInlineRelations = identifyRelationsExcludedFromInline(query)
    val initIDBTable = update_idb_relation_table(query)(env)
    val startEnv = optimiserEnv(env.relationTable ++ initIDBTable, env.fully_inlined_table, nonInlineRelations, env.query_depth)

    val (inlinedQuery, finalEnv) = query.rules.foldLeft((List[idbOptIR](), startEnv)) {
      case ((inlinedRules, nenv), cur_rule) => {
        // inline
        val inlinedrule = inlineRule(cur_rule)(nenv)
        // eliminate self join
        val unifiedrule = unifyExplicitSelfJoin(datalogQueryOptIR(List(inlinedrule)))(nenv).rules.head
        // eliminate contradictory condition
        val consistentJoinRule = removeContradictoryEquality(unifiedrule)(nenv) match {
          case Some(rule) => rule
          case None => NonDefOptIR
        }
        val (finalisedRule, hasRuleBeenRemoved) = consistentJoinRule match {
          case NonDefOptIR => (NonDefOptIR, true)
          case rule => removeContradictoryJoins(rule)(nenv) match {
            case Some(rule) => {
              val nrule = removeRedundantUnionNodeJoins(rule)(nenv)
              (nrule, false)
            }
            case None => (NonDefOptIR, true)
          }
        }

        if (hasRuleBeenRemoved) {
          val oldRelationInfo = nenv.relationTable(get_rule_name(cur_rule))
          val newRelationInfo = RelationInfo(oldRelationInfo.rel_type, 0, oldRelationInfo.decl, None)
          val newRelationTable = nenv.relationTable + (get_rule_name(cur_rule) -> newRelationInfo)
          (inlinedRules, optimiserEnv(newRelationTable, nenv.fully_inlined_table, nenv.dont_inline, nenv.query_depth))
        } else {
          val newDef = transformIdbOptIRToDef(finalisedRule)
          val newType = if (newDef.size == 1) {
            relationTypeEnum.singleIDB
          } else {
            relationTypeEnum.unionIDB
          }
          val oldRelationInfo = nenv.relationTable(get_rule_name(cur_rule))
          val newRelationInfo = RelationInfo(newType, newDef.size, oldRelationInfo.decl, Some(newDef))
          val newRelationTable = nenv.relationTable + (get_rule_name(cur_rule) -> newRelationInfo)
          (inlinedRules :+ finalisedRule, optimiserEnv(newRelationTable, nenv.fully_inlined_table, nenv.dont_inline, nenv.query_depth))
        }
      }
    }
    (datalogQueryOptIR(inlinedQuery), finalEnv)
  }

  def transformIdbOptIRToDef(rule: idbOptIR): List[Def] = rule match {
    case s_rule: singleNormDefOptIR => List(Def(s_rule.a, s_rule.b))
    case u_rule: unionNormDefOptIR => u_rule.branches.map(rule => Def(rule.a, rule.b))
    case nontc_rule: ntClosureDefOptIR => nontc_rule.branches.map(rule => Def(rule.a, rule.b))
    case tc_rule: tClosureDefOptIR => tc_rule.branches.map(rule => Def(rule.a, rule.b))
  }

  def inlineRule(rule: idbOptIR)(env: optimiserEnv): idbOptIR = rule match {
    case s_rule: singleNormDefOptIR => inlineRuleHelper(s_rule)(env) match {
      case rule :: Nil => rule
      case rules => unionNormDefOptIR(rules)
    }
    case u_rule: unionNormDefOptIR => u_rule.branches.flatMap(rule => inlineRuleHelper(rule)(env)) match {
      case rule :: Nil => rule
      case rules => unionNormDefOptIR(rules)
    }
    case nontc_rule: ntClosureDefOptIR => nontc_rule
    case tc_rule: tClosureDefOptIR => tc_rule
  }

  def inlineRuleHelper(rule: singleNormDefOptIR)(env: optimiserEnv): List[singleNormDefOptIR] = {
    val inlineCandidates = fetchInlineCandidates(rule.b)(env)
    val remainAtoms = rule.b.filterNot(inlineCandidates.contains(_))
    val inlinedBodies = inlineCandidates.map(relation => {
      inlineRelation(relation, rule.b)(env)
    })
    generateCombinations(inlinedBodies).map(inlinedBody => inlinedBody ++ remainAtoms)
      .map(finalBody => singleNormDefOptIR(rule.a, finalBody.distinct))
  }

  def inlineRelation(relation: Relation, ruleBody: List[Atom])(env: optimiserEnv): List[List[Atom]] = {
    val relationDefs = env.relationTable.find(e => e._1 == relation.a) match {
      case Some(value) => value._2.definition.get
      case None => throw new Exception(s"Error: Can not find definition of relation $relation")
    }
    relationDefs.map(relationDef => getRenamedRelationDef(relation, ruleBody, relationDef))
  }

  /** rename the relation to be inlind in the way that only variable that collide with the current rule will be
   * shadowed, otherwise keep them as is.
   * */
  def getRenamedRelationDef(cur_rel: Relation, cur_rel_body: List[Atom], rel_def: Def): List[Atom] = {
    val renamedVarMap = getRenamedVariableMap(rel_def, cur_rel_body)
    val renamed_body = ruleDefinitionSafeRenaming(cur_rel, rel_def)(renamedVarMap)
    renamed_body
  }

  def getRenamedVariableMap(rel_def: Def, cur_rel_body: List[Atom]): Map[String, String] = {
    resetFreshVarIndex()
    val vars_projected = (rel_def.a.x).distinct
    val vars_in_body = rel_def.b.flatMap(get_varname_from_atom).distinct
    val vars_only_in_body = vars_in_body.filterNot(vars_projected.contains(_))
    val vars_in_cur_rule = cur_rel_body.flatMap(get_varname_from_atom).distinct
    val unprojected_var_not_collide_with_current_rule = vars_only_in_body.filterNot(vars_in_cur_rule.contains(_))
    vars_in_body.foldLeft(Map.empty[String, String]) {
      (acc_map, cur_name) =>
        cur_name match {
          case s if unprojected_var_not_collide_with_current_rule.contains(s) => acc_map + (s -> s)
          case s => acc_map + (s -> freshVarName(s))
        }
    }
  }

  def ruleDefinitionSafeRenaming(cur_relation: Relation, rel_def: Def)(rename_map: Map[String, String]): List[Atom] = {
    // cur_relation: relation to be inlined, i.e. A(a, b, c)
    // rel_def: relation we find definition A(id, x1, x2)
    // we find the binding of projected variables id -> a, x1-> b, x2 -> c and update the map
    // and create the final rule with name substituted
    val variable_binding = rel_def.a.x.zip(cur_relation.x)
    val updated_var_map = variable_binding.foldLeft(rename_map) {
      case (acc_map, (key, value)) => acc_map + (key -> value)
    }
    val body_renamed = rel_def.b.map(atom => rename_atom(atom)(updated_var_map))
    body_renamed
  }

  /** utility function to generate all combinations of elements
   * from multiple groups (where each group is a list),
   * */
  def generateCombinations(groups: List[List[List[Atom]]]): List[List[Atom]] = {
    groups match {
      case Nil => List(List())
      case head :: tail => for {
        group <- head
        comb <- generateCombinations(tail)
      } yield group ++ comb // Concatenating lists
    }
  }


  def fetchInlineCandidates(body: List[Atom])(env: optimiserEnv): List[Relation] = {
    val inlineCandidates = body.collect { case r: Relation if !env.dont_inline.contains(r.a) => r }
    inlineCandidates.filter(relation => {
      val rel_type = env.relationTable(relation.a).rel_type
      rel_type == relationTypeEnum.unionIDB | rel_type == relationTypeEnum.singleIDB
    })
  }


  /** todo: remove rule involving x = a, x = b and a != b, contradictory condition
   * here, we restrict a and b to be same field from same EDB, Relation, for example,
   * NOTE: effectively this is self contradictory join too!!!
   * country = countryX, country = countryY, Country(countryX, name, _), Country(countryY, name1, _),
   * a rule that includes the above atoms should be eliminated
   * this restriction is needed because otherwise some legit equality will also be eliminated if not careful!!
   */

  def removeContradictoryEquality(rule: idbOptIR)(env: optimiserEnv): Option[idbOptIR] = rule match {
    case s_rule: singleNormDefOptIR => {
      val result = removeContradictoryEqualityHelper(s_rule)(env)
      result.headOption
    }
    case u_rule: unionNormDefOptIR => {
      val result = u_rule.branches.flatMap(s_rule => removeContradictoryEqualityHelper(s_rule)(env))
      if (result.isEmpty) {
        None
      } else if (result.size == 1) {
        Some(result.head)
      } else {
        Some(unionNormDefOptIR(result))
      }
    }
    case nontc_rule: ntClosureDefOptIR => Some(nontc_rule)
    case tc_rule: tClosureDefOptIR => Some(tc_rule)
  }

  def removeContradictoryEqualityHelper(s_rule: singleNormDefOptIR)(env: optimiserEnv): List[singleNormDefOptIR] = {
    val relationIdMap = s_rule.b.collect {case rel: Relation if env.relationTable(rel.a).rel_type == relationTypeEnum.singleNodeEDB => rel}.groupBy(_.a).map {
      case (key, listOfRelations) => key -> listOfRelations.map(_.x.head)
    } // for ex. Map of Country -> [Country(x, ..), Country(y, ..)]
    val eq_groups = s_rule.b.collect { case rel@CmpRel("=", _, _: DlVar) => rel }.groupBy(_.x1) // all x = a, x = b gets grouped by together
    val contra_eq_keys = eq_groups.filter { case (_, list) => list.size > 1 }.keys.toList // get a list of var that has multiple eq conditions

    def existsContraSelfJoin(identityMap: Map[String, List[String]], varsList: List[String]): Boolean = {
      identityMap.exists {
        case (_, value) => varsList.forall(value.contains)
      }
    }
    val existContradiction = contra_eq_keys.exists(key => {
      val vars_list = eq_groups(key).map(item => item.x2.asInstanceOf[DlVar].name) // for x = a, x = b, this gives a list of [a, b]
      existsContraSelfJoin(relationIdMap, vars_list)
    })

    if (existContradiction) {
      List()
    } else {
      List(s_rule)
    }
  }

  /** this should be done after self redundant join
   */
  def removeContradictoryJoins(rule: idbOptIR)(env: optimiserEnv): Option[idbOptIR] = rule match {
    case s_rule: singleNormDefOptIR => {
      val result = removeContradictoryJoinsHelper(s_rule)(env)
      if (result.isEmpty) {
        None
      } else if (result.size == 1) {
        Some(result.head)
      } else {
        Some(unionNormDefOptIR(result))
      }
    }
    case u_rule: unionNormDefOptIR => {
      val result = u_rule.branches.flatMap(s_rule => removeContradictoryJoinsHelper(s_rule)(env))
      if (result.isEmpty) {
        None
      } else if (result.size == 1) {
        Some(result.head)
      } else {
        Some(unionNormDefOptIR(result))
      }
    }
    case nontc_rule: ntClosureDefOptIR => Some(nontc_rule) // we do not do anything to variable-length path
    case tc_rule: tClosureDefOptIR => Some(tc_rule)
  }

  def removeContradictoryJoinsHelper(rule: singleNormDefOptIR)(env: optimiserEnv): List[singleNormDefOptIR] = {
    /** if any two node relation share the same id but with difference relation name
     * and both relation are single idb, then remove this rule
     * */
    // get a list of single edb relation
    val singleEdbRelationsList = rule.b.collect { case r: Relation if env.relationTable(r.a).rel_type == relationTypeEnum.singleNodeEDB => r }

    def hasSharedId(relations: List[Relation]): Boolean = {
      val identityMap = scala.collection.mutable.Map[String, String]()
      relations.foreach { relation =>
        if (identityMap.contains(relation.x.head) && identityMap(relation.x.head) != relation.a) {
          return true
        }
        identityMap(relation.x.head) = relation.a
      }
      return false
    }

    if (hasSharedId(singleEdbRelationsList)) {
      List() // eliminate this rule
    } else {
      List(rule)
    }
  }


  def removeRedundantUnionNodeJoins(rule: idbOptIR)(env: optimiserEnv): idbOptIR = rule match {
    case s_rule: singleNormDefOptIR => removeRedundantUnionNodeJoinsHelper(s_rule)(env)
    case u_rule: unionNormDefOptIR => {
      unionNormDefOptIR(u_rule.branches.map(s_rule => removeRedundantUnionNodeJoinsHelper(s_rule)(env)))
    }
    case nontc_rule: ntClosureDefOptIR => nontc_rule
    case tc_rule: tClosureDefOptIR => tc_rule
  }

  def removeRedundantUnionNodeJoinsHelper(rule: singleNormDefOptIR)(env: optimiserEnv): singleNormDefOptIR = {
    /** if any two node relation share the same id but with difference relation name
     * and one relation is a union node edb, if the other single node edb is contained in
     * part of the union node edb definition, then only keep the subset relation
     * i.e. Post(m,...), Message(m, ...) => Post(m, ...), Post(m, ...)
     * should be executed after self-join unification AND contradictory join removal
     * */
    val (nodeRelations, otherItems) = rule.b.partition {
      case r: Relation => List(relationTypeEnum.singleNodeEDB, relationTypeEnum.unionNodeEDB).contains(env.relationTable(r.a).rel_type)
      case _ => false
    }

    val newNodeRelations = nodeRelations.collect { case r: Relation => r }.groupBy(_.x.head).map {
      case (joinKey, relationList) if relationList.size > 1 => {
        val unionNode = relationList.collect { case node if env.relationTable(node.a).rel_type == relationTypeEnum.unionNodeEDB => node } match {
          case Nil => throw new Exception(s"Error pattern in Rule: ${rule}")
          case unionNode :: Nil => unionNode
          case other => throw new Exception(s"Error pattern ${other} in Rule: ${rule}")
        } // assume only one
        val singleNode = relationList.filterNot(_ == unionNode)
        val updatedRelationList = env.relationTable(unionNode.a).definition match {
          case Some(defs) => {
            val relNamesInSingleNode = singleNode.map(r => r.a).toSet
            val originalDecl = defs.head.a
            val defToUse = defs.flatMap(d => d.b).collect { case r: Relation if relNamesInSingleNode.contains(r.a) => r } match {
              case defNode :: Nil => defNode
              case other => throw new Exception(s"Error: ${other} is matched when decoupling union Node")
            }
            val renamedDecoupledNode = rename_atom(defToUse)(originalDecl.x.zip(unionNode.x).toMap)
            renamedDecoupledNode +: singleNode
          }
          case None => throw new Exception("Error: definition is not found for ")
        }
        updatedRelationList
      }
      case (joinKey, relationList) => relationList
    }.toList.flatten

    val newBody = newNodeRelations ++ otherItems
    singleNormDefOptIR(rule.a, newBody)
  }

  def dead_relation_elimination(query: List[idbOptIR])(env: optimiserEnv): datalogQueryOptIR = {
    val last_rule = query.last
    val init_counter_map = build_init_counter_map(query) + (get_rule_name(last_rule) -> (1, true))
    val dependency_map = build_relation_dependency_dfs(last_rule, query, init_counter_map)(env)
    // eliminate dead relation
    val compressed_rules = query.collect {
      case rule if dependency_map(get_rule_name(rule))._1 != 0 => rule
    }
    datalogQueryOptIR(compressed_rules)
  }

  def build_init_counter_map(query: List[idbOptIR]): Map[String, (Int, Boolean)] = {
    query.map(rule => {
      val rule_name = get_rule_name(rule)
      (rule_name -> (0, false))
    }).toMap
  }

  def build_relation_dependency_dfs(cur_rule: idbOptIR, definition_map: List[idbOptIR], counter_map: Map[String, (Int, Boolean)])
                                   (env: optimiserEnv): Map[String, (Int, Boolean)] = {
    val list_relations_original = get_distinct_idb_name_from_rule(cur_rule)(env)
    val cur_rule_name = get_rule_name(cur_rule)
    val list_relations_used = list_relations_original.filter(_ != cur_rule_name) // to avoid TC():- ...TC() stack overflow of recursive rule
    if (list_relations_used.isEmpty) {
      counter_map
    } else {
      // update map
      val updated_map = list_relations_used.foldLeft(counter_map) {
        (acc_map, cur_name) => {
          if (!acc_map(cur_name)._2) {
            acc_map + (cur_name -> (1, true))
          } else {
            acc_map
          }
        }
      }
      val list_dependent_rules = list_relations_used.flatMap(name => get_definition_from_the_group(name, definition_map))
      list_dependent_rules.foldLeft(updated_map) {
        (acc_map, cur_rule) => {
          val temp = build_relation_dependency_dfs(cur_rule, definition_map, acc_map)(env)
          temp
        }
      }
    }
  }


  def get_varname_from_atom(atom: Atom): List[String] = atom match {
    case Relation(_, args) => args
    case CmpRel(_, DlVar(x1), x2: DlVar) => List(x1, x2.name).distinct
    case CmpRel(_, DlVar(x1), x2: Term) => x1 +: get_varname_from_term(x2)
    case Neg(relation) => get_varname_from_atom(relation)
    case _ => List()
  }

  def get_varname_from_term(term: Term): List[String] = term match {
    case Sum(arg, atom) => (arg +: get_varname_from_atom(atom)).distinct
    case Min(arg, atom) => (arg +: get_varname_from_atom(atom)).distinct
    case Count(args) => args.flatMap(get_varname_from_atom).distinct
    case Arith(_, t1, t2) => (get_varname_from_term(t1) ++ get_varname_from_term(t2)).distinct
    case _ => List()
  }

  def rename_atom(atom: Atom)(rename_map: Map[String, String]): Atom = atom match {
    case Relation(name, args) => Relation(name, args.map(n => rename_map.getOrElse(n, "_")))
    // case CmpRel(op, x1: String, x2: DlVar) => CmpRel(op, rename_map(x1), DlVar(rename_map(x2.name)))
    // case CmpRel(op, x1: String, x2: CmpRel) => CmpRel(op, rename_map(x1), rename_atom(x2)(rename_map))
    // case CmpRel(op, x1: String, other: Atom) => CmpRel(op, rename_map(x1), rename_atom(other)(rename_map))
    // case CmpRel(op, x1: String, nonAtom) => CmpRel(op, rename_map(x1), nonAtom)
    case CmpRel(op, t1, t2) => CmpRel(op, rename_term(t1)(rename_map), rename_term(t2)(rename_map))
    case Neg(relation) => Neg(rename_atom(relation)(rename_map).asInstanceOf[Relation])
    case other => other
  }

  def rename_term(term: Term)(rename_map: Map[String, String]): Term = term match {
    case DlVar(x) => DlVar(rename_map(x))
    case Sum(arg, atom) => Sum(rename_map(arg), rename_atom(atom)(rename_map))
    case Min(arg, atom) => Min(rename_map(arg), rename_atom(atom)(rename_map))
    case Count(args) => Count(args.map(atom => rename_atom(atom)(rename_map)))
    case _ => term
  }


  def get_definition_from_the_group(name: String, acc_rules: List[idbOptIR]): Option[idbOptIR] = {
    acc_rules.find(rule => get_rule_name(rule) == name)
  }

  def get_distinct_idb_name_from_rule(rule: idbOptIR)(env: optimiserEnv): List[String] = rule match {
    case singleNormDefOptIR(_, body) => {
      val list_relations_in_agg = get_relations_in_agg_and_neg(body).toList
      val list_relations_outside = get_outside_relations(body).map(_.a)
      val list_all_relations = (list_relations_in_agg ++ list_relations_outside).distinct
      val only_idbs = list_all_relations.filter(name => {
        val rel_type = env.relationTable(name).rel_type
        rel_type == relationTypeEnum.unionIDB | rel_type == relationTypeEnum.singleIDB |
          rel_type == relationTypeEnum.tcIDB | rel_type == relationTypeEnum.nontcIDB
      })
      only_idbs
    }
    case unionNormDefOptIR(branches) => branches.flatMap(b => get_distinct_idb_name_from_rule(b)(env)).distinct
    case ntClosureDefOptIR(branches) => branches.flatMap(b => get_distinct_idb_name_from_rule(b)(env)).distinct
    case tClosureDefOptIR(branches) => branches.flatMap(b => get_distinct_idb_name_from_rule(b)(env)).distinct
  }

  def get_relations_in_agg_and_neg(atoms: List[Atom]): Set[String] = {
    atoms.flatMap {
      case Neg(relation) => Some(relation.a)
      case CmpRel(_, _, term) => get_relations_in_agg_and_neg_term(List(term))
      case _ => None
    }.toSet
  }

  def get_relations_in_agg_and_neg_term(terms: List[Term]): Set[String] = {
    terms.flatMap {
      case Sum(_, relation: Relation) => Some(relation.a)
      case Min(_, relation: Relation) => Some(relation.a)
      case Count(elems) => elems.collectFirst { case r: Relation => r.a }
      case Arith(_, _, t2) => get_relations_in_agg_and_neg_term(List(t2))
      case _ => None
    }.toSet
  }

  def get_outside_relations(atoms: List[Atom]): List[Relation] = {
    atoms.collect { case r: Relation => r }
  }

  /** Return a list of relation names to be excluded from inlining
   * */
  def identifyRelationsExcludedFromInline(query: datalogQueryOptIR): Set[String] = {
    query.rules.flatMap {
      case s: singleNormDefOptIR => {
        val relationsToExcludeFromInline = get_relations_in_agg_and_neg(s.b)
        if (relationsToExcludeFromInline.nonEmpty) {
          relationsToExcludeFromInline + s.a.a
        } else {
          relationsToExcludeFromInline
        }
      }
      case u: unionNormDefOptIR => {
        u.branches.flatMap(brch => {
          val relationsToExcludeFromInline = get_relations_in_agg_and_neg(brch.b)
          if (relationsToExcludeFromInline.nonEmpty) {
            relationsToExcludeFromInline + brch.a.a
          } else {
            relationsToExcludeFromInline
          }
        })
      }
      case tc: tClosureDefOptIR => Set(get_rule_name(tc))
      case nontc: ntClosureDefOptIR => Set(get_rule_name(nontc))
    }.toSet
  }


  def get_rule_name(rule: idbOptIR): String = rule match {
    case s: singleNormDefOptIR => s.a.a
    case u: unionNormDefOptIR => u.branches.head.a.a
    case nt: ntClosureDefOptIR => nt.branches.head.a.a
    case tc: tClosureDefOptIR => tc.branches.head.a.a
  }

  /** Unify attribute access for self-join, i.e.
   * Person(a,b,_,_),Person(a,_,c,_),Person(a,_,_,d) are merged as Person(a,b,c,d)
   * */
  def unifyExplicitSelfJoin(query: datalogQueryOptIR)(env: optimiserEnv): datalogQueryOptIR = {
    val relation_table = env.relationTable
    val unified_rules: List[idbOptIR] = query.rules.map {
      case s: singleNormDefOptIR => _applyUnificationToSingleDef(s)(relation_table)
      case u: unionNormDefOptIR =>
        unionNormDefOptIR(u.branches.map(r => _applyUnificationToSingleDef(r)(relation_table)))
      case nt: ntClosureDefOptIR =>
        ntClosureDefOptIR(nt.branches.map(r => _applyUnificationToSingleDef(r)(relation_table)))
      case tc: tClosureDefOptIR =>
        tClosureDefOptIR(tc.branches.map(r => _applyUnificationToSingleDef(r)(relation_table)))
    }
    datalogQueryOptIR(unified_rules)
  }

  def _applyUnificationToSingleDef(rule: singleNormDefOptIR)(relation_table: Map[String, RelationInfo]): singleNormDefOptIR = {
    val unified_body = _unifyExplicitSelfJoin(rule.b)(relation_table)
    singleNormDefOptIR(rule.a, unified_body)
  }

  def _unifyExplicitSelfJoin(body: List[Atom])(relation_table: Map[String, RelationInfo]): List[Atom] = {
    // Separate relations from other constructs
    val (relations, others) = body.partition {
      case _: Relation => true
      case _ => false
    }
    // Group relations by their names
    val groupedRelations = relations.collect { case r: Relation => r }.groupBy(_.a).values.toList
    // For each grouped relation
    val merged_flattened_relations = groupedRelations.flatMap(group => {
      val relation_type = lookup_relation_type(group.head.a, relation_table)
      relation_type match {
        case relationTypeEnum.singleNodeEDB | relationTypeEnum.unionNodeEDB => merge_node_edb(group)
        case relationTypeEnum.singleEdgeEDB | relationTypeEnum.unionEdgeEDB => merge_edge_edb(group)
        case relationTypeEnum.singleIDB | relationTypeEnum.unionIDB => merge_duplicate_idb(group)
        case _ => group
      }
    })
    val optimised_body = merged_flattened_relations ++ others
    optimised_body
  }

  def lookup_relation_type(name: String, relation_table: Map[String, RelationInfo]): relationTypeEnum.Value = {
    relation_table(name).rel_type
  }

  def merge_node_edb(relation_list: List[Relation]): List[Atom] = {
    val self_joined_groups = relation_list.groupBy(_.x.headOption).values.toList
    // i.e. [[Person(p, a, _, _), Person(p, _, b, _)]]
    self_joined_groups.flatMap {
      case group if group.size == 1 => group
      case group if group.size > 1 =>
          val init_attr = group.head.x.map(_ => "_")
          val (final_attr, final_eq) = group.foldLeft((init_attr, List[CmpRel]())) {
            case ((acc_attr, equality_list), cur_rel) => {
              val updated_list = acc_attr.zip(cur_rel.x).map {
                case ("_", "_") => ("_", None)
                case ("_", right) => (right, None)
                case (left, "_") => (left, None)
                case (left, right) if left == right => (right, None)
                case (left, right) if left != right => (left, Some(CmpRel("=", DlVar(left), DlVar(right))))
              }
              val updated_attr = updated_list.map(_._1)
              val updated_equality = updated_list.flatMap(_._2)
              (updated_attr, equality_list ++ updated_equality)
            }
          }
          List(Relation(group.head.a, final_attr)) ++ final_eq
    }
  }

  def merge_edge_edb(relation_list: List[Relation]): List[Atom] = {
    // for now, only merge when id1, id2, id are the same
    val self_joined_groups = relation_list.groupBy(relation => relation.x.take(3)).values.toList
    self_joined_groups.flatMap {
      case group if group.size == 1 => group
      case group if group.size > 1 =>
        val init_attr = group.head.x.map(_ => "_")
        val (final_attr, final_eq) = group.foldLeft((init_attr, List[CmpRel]())) {
          case ((acc_attr, equality_list), cur_rel) => {
            val updated_list = acc_attr.zip(cur_rel.x).map {
              case ("_", "_") => ("_", None)
              case ("_", right) => (right, None)
              case (left, "_") => (left, None)
              case (left, right) if left == right => (right, None)
              case (left, right) if left != right => (left, Some(CmpRel("=", DlVar(left), DlVar(right))))
            }
            val updated_attr = updated_list.map(_._1)
            val updated_equality = updated_list.flatMap(_._2)
            (updated_attr, equality_list ++ updated_equality)
          }
        }
        List(Relation(group.head.a, final_attr)) ++ final_eq
    }
  }

  def merge_duplicate_idb(relation_list: List[Relation]): List[Relation] = {
    // remove duplicate Match(a,c,b,d), Match(a,c,b,d)
    val relation_size = relation_list.head.x.size
    val self_joined_groups = relation_list.groupBy(relation => relation.x.take(relation_size)).values.toList
    self_joined_groups.flatMap {
      case group if group.size == 1 => group
      case group if group.size > 1 => List(group.head)
    }
  }

  var globalVarId = 0

  def resetFreshVarIndex(): Unit = {
    globalVarId = 0
  }

  def freshVarName(name: String): String = {
    if (name == "_") {
      "_"
    } else {
      val new_name = s"${name}_${globalVarId}"
      globalVarId += 1
      new_name
    }
  }


}
