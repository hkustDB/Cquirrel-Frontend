import java.util.List;

public class AggregateProcessFunction extends ProcessFunction {
    private final String name;
    private final List<String> thisKey;
    private final List<String> nextKey;


    public AggregateProcessFunction(String name, List<String> thisKey, List<String> nextKey) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;


    }
}
