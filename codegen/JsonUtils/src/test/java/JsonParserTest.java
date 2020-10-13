import org.junit.jupiter.api.Test;

import java.io.File;

class JsonParserTest {
    private final String resourceFolder = new File("src"+ File.separator +"test"+ File.separator +"resources").getAbsolutePath();

    @Test
    void q6json() throws Exception {
        Node node = JsonParser.parse(resourceFolder + File.separator + "Q6.json");
    }
}
