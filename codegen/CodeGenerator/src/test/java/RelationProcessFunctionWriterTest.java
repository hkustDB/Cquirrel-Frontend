import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class RelationProcessFunctionWriterTest {

    @Test
    void q6() throws Exception {
        Node node = JsonParser.parse(new File("src"+ File.separator +"test"+ File.separator +"resources").getAbsolutePath() + File.separator + "Q6.json");
        RelationProcessFunctionWriter rpfw = new RelationProcessFunctionWriter(node.getRelationProcessFunction());
        rpfw.write("src"+ File.separator +"test"+ File.separator +"resources");
    }

    @AfterEach
    void message() {
        System.out.println("inspect the generated class manually at the specified path and remove it");
    }
}
