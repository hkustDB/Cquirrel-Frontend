import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CodeGenTest {
    private final String JSON_FILE_NAME = "sample.json";
    private String JSON_FILE_PATH;
    private String OUTPUT_DIRECTORY;
    private final CodeGen codeGen = new CodeGen();

    @BeforeAll
    void setup() throws Exception {
        URL fileUrl = getClass().getClassLoader().getResource(JSON_FILE_NAME);
        requireNonNull(fileUrl);
        File file = Paths.get(fileUrl.toURI()).toFile();
        JSON_FILE_PATH = file.getAbsolutePath();
        OUTPUT_DIRECTORY = file.getParent();
    }

    @Test
    void mainTest() {
        String[] args = getArgs();
        codeGen.main(args);
    }

    @Test
    void invalidJsonFile() {
        String[] args = getArgs();
        args[0] = args[0].substring(0, args[0].length() - 2);
        exceptionTest(args, "Path provided isn't for a .json file");
    }

    @Test
    void noJsonFile() {
        String[] args = getArgs();
        args[0] = args[0].replace(JSON_FILE_NAME, "file.json");
        exceptionTest(args, "Unable to find JSON file");
    }

    @Test
    void nullOrEmptyArgs() {
        exceptionTest(null, "Argument cannot be null or empty");
        String[] args = getArgs();
        args[0] = "";
        exceptionTest(args, "Argument cannot be null or empty");
    }

    @Test
    void extraArgs() {
        String[] args = getArgs();
        args[2] = "";
        exceptionTest(null, "Expecting exactly 2 input strings: JSON file path and flink task manager address");
    }

    @Test
    void invalidOutputDir() {
        String[] args = getArgs();
        args[1] = "invalidDirectory_";
        exceptionTest(args, "output directory must exist and must be a directory");
    }

    private void exceptionTest(String[] args, String exceptionMessage) {
        try {
            codeGen.main(args);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(exceptionMessage));
        }
    }

    private String[] getArgs() {
        String[] args = new String[2];
        args[0] = JSON_FILE_PATH;
        args[1] = OUTPUT_DIRECTORY;
        return args;
    }
}
