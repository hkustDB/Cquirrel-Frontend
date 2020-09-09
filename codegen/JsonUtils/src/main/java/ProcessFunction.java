import java.util.List;

public abstract class ProcessFunction {
    private final String name;
    private final List<String> thisKey;
    private final List<String> nextKey;

    public ProcessFunction(final String name, final List<String> thisKey, final List<String> nextKey) {
        CheckerUtils.checkNullOrEmpty(name,"name");
        CheckerUtils.checkNullOrEmpty(thisKey,"thisKey");
        CheckerUtils.checkNullOrEmpty(nextKey,"nextKey");
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;
    }
}
