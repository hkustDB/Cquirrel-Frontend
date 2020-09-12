import java.util.List;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunction extends ProcessFunction {
    private final String name;
    private final List<String> thisKey;
    private final List<String> nextKey;

    private final Expression computation;
    private final Operator aggregation;
    private final Class valueType;

    public AggregateProcessFunction(String name, List<String> thisKey, List<String> nextKey, Expression computation, Operator aggregation, Class valueType) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;

        requireNonNull(computation);
        this.computation = computation;
        requireNonNull(aggregation);
        this.aggregation = aggregation;
        requireNonNull(valueType);
        this.valueType = valueType;
    }
}
