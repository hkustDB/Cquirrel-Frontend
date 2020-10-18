import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class AggregateProcessFunctionWriterTest {

    @Test
    void q6() throws Exception {
        Node node = JsonParser.parse(new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath() + File.separator + "Q6.json");
        AggregateProcessFunctionWriter agfw = new AggregateProcessFunctionWriter(node.getAggregateProcessFunction());
        agfw.write("src" + File.separator + "test" + File.separator + "resources");
    }

    @AfterEach
    void teardown() {
        System.out.println("inspect the generated class manually at the specified path and remove it");
    }

}
