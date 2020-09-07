import com.sun.istack.internal.Nullable;

import java.util.List;
import java.util.Map;

public class Node {
    private final String nodeName;
    private final String relationName;
    private final String distributedKey;
    private final String nextDistributedKey;
    private final Position position;
    private final boolean isLast;
    @Nullable
    private final Map<String, String> renaming;
    //TODO: make immutable
    private final List<SelectCondition> selectConditions;

    public Node(final String nodeName, final String relationName, final String distributedKey,
                final String nextDistributedKey, Position position, boolean isLast,
                Map<String, String> renamings, List<SelectCondition> selectConditions) {

        CheckerUtils.checkNullOrEmpty(nodeName, "nodeName");
        this.nodeName = nodeName;
        CheckerUtils.checkNullOrEmpty(relationName, "relationName");
        this.relationName = relationName;
        CheckerUtils.checkNullOrEmpty(distributedKey, "distributedKey");
        this.distributedKey = distributedKey;
        CheckerUtils.checkNullOrEmpty(nextDistributedKey, "nextDistributedKey");
        this.nextDistributedKey = nextDistributedKey;

        this.renaming = renamings;
        this.position = position;

        CheckerUtils.checkNullOrEmpty(selectConditions, "selectConditions");
        this.selectConditions = selectConditions;

        this.isLast = isLast;
    }

}
