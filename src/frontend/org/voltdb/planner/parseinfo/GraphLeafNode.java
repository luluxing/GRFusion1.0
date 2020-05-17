/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner.parseinfo;

import java.util.*;
import java.util.function.Predicate;

import org.voltdb.expressions.*;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.StmtEphemeralTableScan;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.types.JoinType;

/**
 * An object of class TableLeafNode is a leaf in a join expression tree.  It
 * represents a table reference.
 */
public class GraphLeafNode extends JoinNode {
    private StmtTargetGraphScan m_graphScan;
    /**
     * Construct a table leaf node
     * @param id - node unique id
     * @param table - join table index
     * @param joinExpr - join expression
     * @param whereExpr - filter expression
     * @param id - node id
     */
    public GraphLeafNode(int id, AbstractExpression joinExpr, AbstractExpression whereExpr,
            StmtTargetGraphScan graphScan) {
        super(id);
        m_joinExpr = joinExpr;
        m_whereExpr = whereExpr;
        m_graphScan = graphScan;
    }

    /**
     * Deep clone
     */
    @Override
    public Object clone() {
        AbstractExpression joinExpr = (m_joinExpr != null) ?
                (AbstractExpression) m_joinExpr.clone() : null;
        AbstractExpression whereExpr = (m_whereExpr != null) ?
                (AbstractExpression) m_whereExpr.clone() : null;
        JoinNode newNode = new GraphLeafNode(m_id, joinExpr, whereExpr, m_graphScan);
        return newNode;
    }

    @Override
    public JoinNode cloneWithoutFilters() {
        JoinNode newNode = new GraphLeafNode(m_id, null, null, m_graphScan);
        return newNode;
    }

    @Override
    public StmtTableScan getTableScan() { return m_graphScan; }

    @Override public String getTableAlias() { return m_graphScan.getTableAlias(); }

    // @Override
    // public void analyzeJoinExpressions(List<AbstractExpression> noneList) {
    //     m_joinInnerList.addAll(ExpressionUtil.uncombineAny(getJoinExpression()));
    //     m_whereInnerList.addAll(ExpressionUtil.uncombineAny(getWhereExpression()));
    // } comment LX

    // rewrite LX
    @Override
    public void analyzeJoinExpressions(AbstractParsedStmt stmt) {
        m_joinInnerList.addAll(ExpressionUtil.uncombineAny(getJoinExpression()));
        m_whereInnerList.addAll(ExpressionUtil.uncombineAny(getWhereExpression()));
    }

    @Override
    protected void collectEquivalenceFilters(Map<AbstractExpression, Set<AbstractExpression>> equivalenceSet, Deque<JoinNode> joinNodes) // modified LX
    {
        if ( ! m_whereInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereInnerList, equivalenceSet);
        }
        // HSQL sometimes tags single-table filters in inner joins as join clauses
        // rather than where clauses? OR does analyzeJoinExpressions correct for this?
        // If so, these CAN contain constant equivalences that get used as the basis for equivalence
        // conditions that determine partitioning, so process them as where clauses.
        if ( ! m_joinInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_joinInnerList, equivalenceSet);
        }
    }

    @Override
    public boolean hasSubqueryScans(){
        return true; // add LX nonsensical return
    }

    @Override
    public void extractEphemeralTableQueries(List<StmtEphemeralTableScan> scans){
        return ; // Add LX nonsensical return
    }
    // Add LX
    @Override
    protected void queueChildren(Deque<JoinNode> joinNodes) {
    }
}
