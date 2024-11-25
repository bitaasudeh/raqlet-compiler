package uk.ac.ed.dal
package raqlet
package transformer

import ir._

object CyphRewrite {

  def rewriteCyphAst(query: CypherQuery)(schema: CyphSchema): CypherQuery = query match {
    case Return(eas) => Return(eas)
    case ClauseQuery(c, q) => {
      val rewrite = c match {
        case m: Match => rewriteMatch(m)(schema)
        case om: OptionalMatch => rewriteOptionalMatch(om)(schema)
        case other => other
      }
      ClauseQuery(rewrite, rewriteCyphAst(q)(schema))
    }
  }

  def rewriteOptionalMatch(om: OptionalMatch)(schema: CyphSchema): Clause = {
    val rewritten = om.matches.map {
      case mat: Match => rewriteMatch(mat)(schema)
      case other => other
    }
    OptionalMatch(rewritten)
  }

  def rewriteMatch(m: Match)(schema: CyphSchema): Clause = {
    /* In the case of (Message)-[something]->(Node)
    * rewrite (Message)-[something]->(Node) as UNIONMATCH(List(EdgePatten1,
    * EdgePattern2))
    */
    val rewrittenMatch = m.p match {
      case NodePattern(vrb, label) => {
        m
      } // single node rewrite
      case EdgePattern(src, trg, vrb, label, dist, direction) => {
        // For later: Now we only consider cases where either src or trg node is union type (but not both!!!)
        if (nodeIsUnionType(src.label)(schema)) {
          val labelsList = getNodeUnionType(src.label)(schema)
          val unionNodes = labelsList.map(label => NodePattern(src.vrb, label.name))
          // deal with *0... and otherwise
          val listOfPatterns = dist match {
            case Some(TransDistance(0)) => {
              val patterns: List[Pattern] = unionNodes.flatMap(nsrc => {
                val ety = EdgeType(LabelInput(nsrc.label), LabelInput(trg.label), LabelInput(label), Seq(), "")
                if (edgeTypeExistInSchema(ety)(schema)) {
                  Some(EdgePattern(nsrc, trg, vrb, label, Some(TransDistance(1)), direction))
                } else {
                  Some(nsrc)
                }
              })
              patterns
            }
            case _ => {
              val patterns: List[Pattern] =  unionNodes.flatMap(nsrc => {
                val ety = EdgeType(LabelInput(nsrc.label), LabelInput(trg.label), LabelInput(label), Seq(), "")
                if (edgeTypeExistInSchema(ety)(schema)) {
                  Some(EdgePattern(nsrc, trg, vrb, label, dist, direction))
                } else {
                  None
                }
              })
              patterns
            }
          }
          UnionMatch(listOfPatterns)
        } else if (nodeIsUnionType(trg.label)(schema)) {
          val labelsList = getNodeUnionType(trg.label)(schema)
          val unionNodes = labelsList.map(label => NodePattern(trg.vrb, label.name))
          val listOfPatterns = dist match {
            case Some(TransDistance(0)) => {
              val patterns: List[Pattern] = unionNodes.flatMap(ntrg => {
                val ety = EdgeType(LabelInput(src.label), LabelInput(ntrg.label), LabelInput(label), Seq(), "")
                if (edgeTypeExistInSchema(ety)(schema)) {
                  Some(EdgePattern(src, ntrg, vrb, label, Some(TransDistance(1)), direction))
                } else {
                  Some(ntrg)
                }
              })
              patterns
            }
            case _ => {
              val patterns: List[Pattern] = unionNodes.flatMap(ntrg => {
                val ety = EdgeType(LabelInput(src.label), LabelInput(ntrg.label), LabelInput(label), Seq(), "")
                if (edgeTypeExistInSchema(ety)(schema)) {
                  Some(EdgePattern(src, ntrg, vrb, label, dist, direction))
                } else {
                  None
                }
              })
              patterns
            }
          }
          UnionMatch(listOfPatterns)
        } else {
          m
        }
      }
    }
    rewrittenMatch
  }

  def nodeIsUnionType(label: String)(schema: CyphSchema): Boolean = {
    schema.types.exists {
      case nt: NodeType if nt.name == label && nt.spec.isInstanceOf[LabelOr] => true
      case _ => false
    }
  }

  def getNodeUnionType(label: String)(schema: CyphSchema): List[LabelInput] = {
    val labels = schema.types.collectFirst {
      case nt: NodeType if nt.name == label && nt.spec.isInstanceOf[LabelOr] => nt.spec.asInstanceOf[LabelOr].list.toList
    }

    labels match {
      case Some(list) => list
      case None => throw new Exception(s"Spec of node $label is not union type")
    }
  }

  def edgeTypeExistInSchema(ety: EdgeType)(schema: CyphSchema): Boolean = {
    val result = schema.types.exists {
      case EdgeType(src, dest, rel, _, _) => (src.name, dest.name, rel.name) == (ety.src.name, ety.dest.name, ety.rel.name)
      case _ => false
    }
    result
  }

}
