import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RelationProcessFunction extends ProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> nextKey;

    private final int childNodes;
    private final boolean isRoot;
    private final boolean isLast;
    @Nullable
    private final Map<String, String> renaming;
    private final List<SelectCondition> selectConditions;

    public RelationProcessFunction(String name, List<String> thisKey, List<String> nextKey, int childNodes,
                                   boolean isRoot, boolean isLast, Map<String, String> renaming,
                                   List<SelectCondition> selectConditions) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;
        if (childNodes < 0)
            throw new RuntimeException("Number of child nodes must be >=0, got: " + childNodes);
        this.childNodes = childNodes;
        this.isRoot = isRoot;
        this.isLast = isLast;
        CheckerUtils.validateNonNullNonEmpty((Collection) renaming, "renaming");
        this.renaming = renaming;
        CheckerUtils.checkNullOrEmpty(selectConditions, "selectConditions");
        this.selectConditions = selectConditions;
    }

    private void validateRenamingMap() {

    }

    @Override
    public String toString() {
        return "RelationProcessFunction{" +
                "name='" + name + '\'' +
                ", thisKey=" + thisKey +
                ", nextKey=" + nextKey +
                ", childNodes=" + childNodes +
                ", isRoot=" + isRoot +
                ", isLast=" + isLast +
                ", renaming=" + renaming +
                ", selectConditions=" + selectConditions +
                '}';
    }
}
