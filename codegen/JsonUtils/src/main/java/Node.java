import static java.util.Objects.requireNonNull;

public class Node {
    private final ProcessFunction relationProcessFunction;
    private final ProcessFunction aggregateProcessFunction;
    private final Configuration configuration;


    public Node(ProcessFunction relationProcessFunction, ProcessFunction aggregateProcessFunction, Configuration configuration) {
        requireNonNull(relationProcessFunction);
        requireNonNull(aggregateProcessFunction);
        requireNonNull(configuration);
        this.relationProcessFunction = relationProcessFunction;
        this.aggregateProcessFunction = aggregateProcessFunction;
        this.configuration = configuration;
    }

    public RelationProcessFunction getRelationProcessFunction() {
        return (RelationProcessFunction) relationProcessFunction;
    }

    public AggregateProcessFunction getAggregateProcessFunction() {
        return (AggregateProcessFunction) aggregateProcessFunction;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public String toString() {
        return "Node{" +
                "relationProcessFunction=" + relationProcessFunction +
                ", aggregateProcessFunction=" + aggregateProcessFunction +
                ", configuration=" + configuration +
                '}';
    }
}
