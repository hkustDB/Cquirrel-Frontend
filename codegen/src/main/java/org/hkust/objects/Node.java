package org.hkust.objects;

import static java.util.Objects.requireNonNull;

public class Node {
    private final ProcessFunction relationProcessFunction;
    private final ProcessFunction aggregateProcessFunction;


    public Node(ProcessFunction relationProcessFunction, ProcessFunction aggregateProcessFunction) {
        requireNonNull(relationProcessFunction);
        requireNonNull(aggregateProcessFunction);
        this.relationProcessFunction = relationProcessFunction;
        this.aggregateProcessFunction = aggregateProcessFunction;
    }

    public RelationProcessFunction getRelationProcessFunction() {
        return (RelationProcessFunction) relationProcessFunction;
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
