import org.junit.jupiter.api.Test;

public class JsonParserTest {
    final String Q6_JSON = "C:\\Users\\Danish Ibrahim\\Desktop\\gui-codegen\\codegen\\JsonUtils\\src\\test\\resources\\Q6.json";

    @Test
    void parseTest() throws Exception {
        JsonParser.parse(Q6_JSON);
    }
}
