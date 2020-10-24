import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CodeGenTest {
    private final String JSON_FILE_NAME = "sample.json";
    private String JSON_FILE_PATH;
    private String TEST_DIRECTORY;
    private final CodeGen codeGen = new CodeGen();

    @BeforeAll
    void setup() {
        final String resourceFolder = new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath();
        JSON_FILE_PATH = resourceFolder + File.separator + JSON_FILE_NAME;
        TEST_DIRECTORY = resourceFolder;
    }

    @Test
    void integrationTest() {
        //test with real arguments
        //verify output --> store generated code for a query that is supports e.g. store the currently being produced code for q6 as a resource to verify against each time and ensure "backwards" compatibility
        //clean up after test
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
        String[] args = getArgs();
        args[0] = "";
        exceptionTest(args, "cannot be null or empty");
    }

    @Test
    void extraArgs() {
        String[] args = new String[6];
        exceptionTest(args, "Expecting exactly 5 input strings");
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
        String[] args = new String[5];
        args[0] = JSON_FILE_PATH;
        args[1] = TEST_DIRECTORY;
        args[2] = TEST_DIRECTORY;
        args[3] = TEST_DIRECTORY;
        args[4] = "File";
        return args;
    }
}
