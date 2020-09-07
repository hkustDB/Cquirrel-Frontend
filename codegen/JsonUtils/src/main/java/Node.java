import com.sun.istack.internal.Nullable;

import java.util.List;
import java.util.Map;

public class Node {
    /**
     * A class representing the metaData provided by the user through the GUI in JSON format, must never be initialized manually
     * Note: this class made on the assumption that we will use Google Gson to populate this class automatically
     * node_name
     * |
     * |----relation
     * |
     * |----distribute key of this node
     * |
     * |----distribute key of next node
     * |
     * |----The number of child nodes
     * |
     * |----If the node is the root node
     * |
     * |----If the node is the leaf node
     * |
     * |----If the node is the last node of the relation
     * |
     * |----If any attributes need to be renamed
     * |
     * |----The select conditions
     */

    private final String nodeName;
    private final String relationName;
    private final String distributedKey;
    private final String nextDistributedKey;
    private final Position position;
    private boolean isLast;
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
    }

}
