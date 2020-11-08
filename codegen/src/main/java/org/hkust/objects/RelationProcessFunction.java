package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;
import org.hkust.schema.Relation;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RelationProcessFunction extends ProcessFunction {
    private final String name;
    private final Relation relation;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> nextKey;

    private final int childNodes;
    private final boolean isRoot;
    private final boolean isLast;
    @Nullable
    private final Map<String, String> renaming;
    private final List<SelectCondition> selectConditions;

    @Nullable
    private RelationProcessFunction parent = null;

    public RelationProcessFunction(String name, String relationName, List<String> thisKey, List<String> nextKey, int childNodes,
                                   boolean isRoot, boolean isLast, Map<String, String> renaming,
                                   List<SelectCondition> selectConditions) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;
        if (childNodes < 0)
            throw new RuntimeException("Number of child nodes must be >=0, got: " + childNodes);
        CheckerUtils.checkNullOrEmpty(relationName, "relationName");
        this.relation = Relation.getRelation(relationName);
        this.childNodes = childNodes;
        this.isRoot = isRoot;
        this.isLast = isLast;
        CheckerUtils.validateNonNullNonEmpty((Collection) renaming, "renaming");
        this.renaming = renaming;
        CheckerUtils.checkNullOrEmpty(selectConditions, "selectConditions");
        this.selectConditions = selectConditions;
    }


    @Override
    public String getName() {
        return name;
    }

    @Nullable
    public List<String> getThisKey() {
        return thisKey;
    }

    @Nullable
    public List<String> getNextKey() {
        return nextKey;
    }

    public int getChildNodes() {
        return childNodes;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public boolean isLast() {
        return isLast;
    }

    public Relation getRelation() {
        return relation;
    }

    @Nullable
    public Map<String, String> getRenaming() {
        return renaming;
    }

    public List<SelectCondition> getSelectConditions() {
        return selectConditions;
    }

    @Override
    public String toString() {
        return "RelationProcessFunction{" +
                "name='" + name + '\'' +
                ", thisKey=" + thisKey +
                ", nextKey=" + nextKey +
                ", childNodes=" + childNodes +
                ", isRoot=" + isRoot +
                ", isLast=" + isLast +
                ", renaming=" + renaming +
                ", selectConditions=" + selectConditions +
                '}';
    }
}
