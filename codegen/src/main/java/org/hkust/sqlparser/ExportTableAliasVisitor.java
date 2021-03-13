package org.hkust.sqlparser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGASTVisitorAdapter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by tom on 20/2/2021.
 * Copyright (c) 2021 tom
 */
public class ExportTableAliasVisitor extends PGASTVisitorAdapter {
    private Map<String, SQLTableSource> aliasMap = new HashMap<String, SQLTableSource>();
    public List<SQLExpr> groupByAttributes;
    public HashSet<SQLSelectItem> selectItem = new HashSet<SQLSelectItem>();
    public HashSet<SQLAggregateExpr> aggregation = new HashSet<>();
    public HashSet<SQLSelectStatement> selectStatement = new HashSet<>();
    public HashSet<String> table = new HashSet<>();
    public boolean visit(SQLExprTableSource x) {
        String alias = x.getAlias();
        table.add(x.getTableName());
        aliasMap.put(alias, x);
        return true;
    }

    @Override
    public boolean visit(SQLSelectGroupByClause x) {
        groupByAttributes = x.getItems();
        return true;
    }

    public boolean visit(SQLAggregateExpr x) {
        aggregation.add(x);
        return true;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        selectItem.add(x);
        return true;
    }


    @Override
    public boolean visit(SQLSelectStatement x) {
        selectStatement.add(x);
        return true;
    }

    public Map<String, SQLTableSource> getAliasMap() {
        return aliasMap;
    }
}
