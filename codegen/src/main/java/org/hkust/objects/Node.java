package org.hkust.objects;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Node {
    private final List<RelationProcessFunction>  relationProcessFunction;
    private final ProcessFunction aggregateProcessFunction;


    public Node(List<RelationProcessFunction> relationProcessFunction, ProcessFunction aggregateProcessFunction) {
        requireNonNull(relationProcessFunction);
        requireNonNull(aggregateProcessFunction);
        this.relationProcessFunction = relationProcessFunction;
        this.aggregateProcessFunction = aggregateProcessFunction;
    }

    public List<RelationProcessFunction> getRelationProcessFunctions() {
        return relationProcessFunction;
    }

    public AggregateProcessFunction getAggregateProcessFunction() {
        return (AggregateProcessFunction) aggregateProcessFunction;
    }

    @Override
    public String toString() {
        return "Node{" +
                "relationProcessFunction=" + relationProcessFunction +
                ", aggregateProcessFunction=" + aggregateProcessFunction +
                '}';
    }
}
