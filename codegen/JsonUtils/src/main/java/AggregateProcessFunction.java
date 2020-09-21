import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunction extends ProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> nextKey;

    private final Expression aggregateValue;
    private final Operator aggregation;
    private final Class valueType;

    public AggregateProcessFunction(String name, List<String> thisKey, List<String> nextKey, Expression aggregateValue,
                                    Operator aggregation, Class valueType) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;

        requireNonNull(aggregateValue);
        this.aggregateValue = aggregateValue;
        requireNonNull(aggregation);
        this.aggregation = aggregation;
        requireNonNull(valueType);
        this.valueType = valueType;
    }

    @Override
    public String toString() {
        return "AggregateProcessFunction{" +
                "name='" + name + '\'' +
                ", thisKey=" + thisKey +
                ", nextKey=" + nextKey +
                ", computation=" + aggregateValue +
                ", aggregation=" + aggregation +
                ", valueType=" + valueType +
                '}';
    }
}
