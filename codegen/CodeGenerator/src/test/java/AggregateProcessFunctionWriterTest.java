import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class AggregateProcessFunctionWriterTest {

    @Test
    void q6() throws Exception {
        Node node = JsonParser.parse("C:\\Users\\Danish Ibrahim\\Desktop\\gui-codegen\\codegen\\JsonUtils\\src\\test\\resources\\Q6.json");
        AggregateProcessFunctionWriter agfw = new AggregateProcessFunctionWriter(node.getAggregateProcessFunction());
        agfw.generateCode("C:\\Users\\Danish Ibrahim\\Desktop");
    }

    @AfterEach
    void message(){
        System.out.println("inspect the generated class manually at the specified path");
    }

}
