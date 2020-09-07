public class JsonData {
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

    //TODO: for complex members consider builder design pattern i.e. populate the simple values automatically using Gson then enrich in the builder
}
