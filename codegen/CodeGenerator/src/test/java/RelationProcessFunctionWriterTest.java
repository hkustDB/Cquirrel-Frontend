import org.junit.jupiter.api.Test;

public class RelationProcessFunctionWriterTest {

    @Test
    void q6() throws Exception {
        //TODO: find a better way to share the test resources of JsonUtils in here
        Node node = JsonParser.parse("C:\\Users\\Danish Ibrahim\\Desktop\\gui-codegen\\codegen\\JsonUtils\\src\\test\\resources\\Q6.json");
        RelationProcessFunctionWriter rpfw = new RelationProcessFunctionWriter(node.getRelationProcessFunction());
        rpfw.generateCode("C:\\Users\\Danish Ibrahim\\Desktop");
    }
}
