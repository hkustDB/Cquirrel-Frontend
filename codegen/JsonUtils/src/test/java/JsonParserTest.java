import org.junit.jupiter.api.Test;

import java.io.File;

class JsonParserTest {
    private final String resourceFolder = new File("src/test/resources").getAbsolutePath();

    @Test
    void q6json() throws Exception {
        JsonParser.parse(resourceFolder + File.separator + "Q6.json");
    }
}
