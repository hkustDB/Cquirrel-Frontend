import org.junit.jupiter.api.Test;

import java.io.File;

public class CodeGeneratorTest {

    @Test
    void q6() throws Exception {
        Node node = JsonParser.parse(new File("src/test/resources").getAbsolutePath() + File.separator + "Q6.json");
        CodeGenerator.generate(node, "src/test/resources");
    }
}
