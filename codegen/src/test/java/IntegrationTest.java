import org.junit.After;
import org.junit.Test;

import java.io.File;

public class IntegrationTest {
    private final String resourceFolder = new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath();


    @Test
    public void q6IntegrationTest() throws Exception {
        integrationTest("Q6.json",
                "file:///home/data/qwangbp/lineitem.tbl",
                "file:///home/data/qwangbp/testQ6.out",
                "file");

        verifyResult();
    }

    private void integrationTest(String jsonFileName, String flinkInput, String flinkOutput, String mode) throws Exception {
        final String jsonFilePath = resourceFolder + File.separator + jsonFileName;
        String[] args = {jsonFilePath, resourceFolder, flinkInput, flinkOutput, mode};
        CodeGen.main(args);
    }

    @After
    public void cleanup() {

    }

    private void verifyResult() {

    }
}
