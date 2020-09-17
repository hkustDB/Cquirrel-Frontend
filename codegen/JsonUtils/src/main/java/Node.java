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

    @Override
    public String toString() {
        return "Node{" +
                "relationProcessFunction=" + relationProcessFunction +
                ", aggregateProcessFunction=" + aggregateProcessFunction +
                '}';
    }
}
