import com.google.common.collect.ImmutableList;
import org.hkust.codegenerator.CodeGenerator;
import org.junit.After;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IntegrationTest {
    private final String RESOURCE_FOLDER = new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath();
    private final String GENERATED_CODE_PATH = RESOURCE_FOLDER + File.separator + CodeGenerator.GENERATED_CODE;
    private final Runtime runtime = Runtime.getRuntime();
    private final String codegenJarPath = new File(CodeGen.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile().getAbsolutePath() + File.separator + "codegen-1.0-SNAPSHOT.jar";

    private final List<String> GENERATED_CODE_JAR_PATHS = ImmutableList.of(
            GENERATED_CODE_PATH + File.separator + "target" + File.separator + "generated-code-1.0-SNAPSHOT.jar",
            GENERATED_CODE_PATH + File.separator + "target" + File.separator + "generated-code-1.0-SNAPSHOT-jar-with-dependencies.jar"
    );

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(GENERATED_CODE_PATH));
    }

    @Test
    public void q6IntegrationTest() throws Exception {
        integrationTest("q6/Q6.json",
                "file:///home/data/qwangbp/lineitem.tbl",
                "file:///home/data/qwangbp/testQ6.out",
                "file",
                false);

        verifyResult("q6", Arrays.asList("Job.scala", "Q6AggregateProcessFunction.scala", "Q6lineitemProcessFunction"));
    }

    private void integrationTest(String jsonFileName, String flinkInput, String flinkOutput, String mode, boolean withJar) throws Exception {
        final String jsonFilePath = RESOURCE_FOLDER + File.separator + jsonFileName;
        if (withJar) {
            execute(runtime, "java -jar " + codegenJarPath + " " + jsonFilePath + " " + RESOURCE_FOLDER + " " + flinkInput + " " + flinkOutput + " " + mode);
        } else {
            String[] args = {jsonFilePath, RESOURCE_FOLDER, flinkInput, flinkOutput, mode};
            CodeGen.main(args);
        }
    }

    private void verifyResult(String expectedResultDirName, List<String> fileNames) {
        GENERATED_CODE_JAR_PATHS.forEach(path -> assertTrue(new File(path).exists()));

        String expectedDirPath = RESOURCE_FOLDER + File.separator + expectedResultDirName;
        String resultDirPath = GENERATED_CODE_PATH + File.separator + "src" + File.separator + "main" + File.separator + "scala" + File.separator + "org" + File.separator + "hkust";

        File pom = new File(GENERATED_CODE_PATH + File.separator + "pom.xml");
        assertTrue(pom.exists() && pom.isFile());
        assertTrue(pom.length() != 0);

        fileNames.forEach(file -> {
            File resultFile = new File(resultDirPath + File.separator + file);
            File expectedFile = new File(expectedDirPath + File.separator + file);
            try {
                boolean passed = FileUtils.contentEquals(resultFile, expectedFile);
                if (!passed) {
                    System.out.println("Result and output file aren't identical for " + file + ". The difference between the files:");
                    printDifference(resultFile, expectedFile);
                    fail();
                }
            } catch (IOException e) {
                throw new RuntimeException("Exception when reading for " + file, e);
            }
        });

    }

    private void printDifference(File resultFile, File expectedFile) throws IOException {
        Process process = Runtime.getRuntime().exec("diff " + resultFile.getAbsolutePath() + " " + expectedFile.getAbsolutePath());
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String output = null;
        while ((output = stdInput.readLine()) != null) {
            System.out.println(output);
        }

        while ((output = stdError.readLine()) != null) {
            System.out.println(output);
        }
    }

    private static void execute(Runtime runtime, String command) throws IOException {
        Process process = runtime.exec(command);

        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));

        BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        System.out.println("Running " + command + ":\n");
        String output = null;
        while ((output = stdInput.readLine()) != null) {
            System.out.println(output);
        }

        System.out.println("Errors of " + command + " (if any):\n");
        while ((output = stdError.readLine()) != null) {
            System.out.println(output);
        }
    }
}
