package org.hkust.objects;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Node {
    private final List<RelationProcessFunction> relationProcessFunctions;
    private final List<AggregateProcessFunction> aggregateProcessFunctions;


    public Node(List<RelationProcessFunction> relationProcessFunctions, List<AggregateProcessFunction> aggregateProcessFunctions) {
        requireNonNull(relationProcessFunctions);
        requireNonNull(aggregateProcessFunctions);
        this.relationProcessFunctions = relationProcessFunctions;
        this.aggregateProcessFunctions = aggregateProcessFunctions;
    }

    public List<RelationProcessFunction> getRelationProcessFunctions() {
        return relationProcessFunctions;
    }

    public List<AggregateProcessFunction>  getAggregateProcessFunctions() {
        return aggregateProcessFunctions;
    }

    @Override
    public String toString() {
        return "Node{" +
                "relationProcessFunction=" + relationProcessFunctions +
                ", aggregateProcessFunction=" + aggregateProcessFunctions +
                '}';
    }
}
