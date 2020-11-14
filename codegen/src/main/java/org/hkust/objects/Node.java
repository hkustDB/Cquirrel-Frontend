package org.hkust.objects;

import com.google.common.collect.Multimap;
import org.hkust.schema.Relation;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Node {
    private final List<RelationProcessFunction> relationProcessFunctions;
    private final List<AggregateProcessFunction> aggregateProcessFunctions;
    private final Multimap<Relation, Relation> joinStructure;


    public Node(List<RelationProcessFunction> relationProcessFunctions, List<AggregateProcessFunction> aggregateProcessFunctions, Multimap<Relation, Relation> joinStructure) {
        this.joinStructure = joinStructure;
        requireNonNull(relationProcessFunctions);
        requireNonNull(aggregateProcessFunctions);
        this.relationProcessFunctions = relationProcessFunctions;
        this.aggregateProcessFunctions = aggregateProcessFunctions;
    }

    public List<RelationProcessFunction> getRelationProcessFunctions() {
        return relationProcessFunctions;
    }

    public List<AggregateProcessFunction> getAggregateProcessFunctions() {
        return aggregateProcessFunctions;
    }

    @Nullable
    public Multimap<Relation, Relation> getJoinStructure() {
        return joinStructure;
    }

    @Override
    public String toString() {
        return "Node{" +
                "relationProcessFunction=" + relationProcessFunctions +
                ", aggregateProcessFunction=" + aggregateProcessFunctions +
                '}';
    }
}
