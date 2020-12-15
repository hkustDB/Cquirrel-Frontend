import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import static java.io.File.separator;

public class GenerateCodeTest {
    private static final ArrayList<Integer> queryArrayList = new ArrayList<>();
    private final String GENERATED_CODE = "generated-code";
    private static final String RESOURCE_FOLDER = new File("src" + separator + "test" + separator + "resources").getAbsolutePath();
    private final String CODEGEN_JAR_PATH = new File(CodeGen.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile().getAbsolutePath() + separator + "codegen-1.0-SNAPSHOT.jar";

    @Before
    public void getQueryArrayList() throws Exception {
        File resourceFolderFile = new File(RESOURCE_FOLDER);
        if (!resourceFolderFile.exists() || !resourceFolderFile.isDirectory()) {
            throw new Exception("resource folder does not exists.");
        }

        File[] resourceFolderFiles = resourceFolderFile.listFiles();
        for (File file : resourceFolderFiles) {
            if (!file.isDirectory() || file.getName().charAt(0) != 'q') {
                continue;
            }
            int queryNum;
            try {
                queryNum = Integer.parseInt(file.getName().substring(1));
            } catch (NumberFormatException e) {
                System.out.println(file.getName() + " is not a query name.");
                continue;
            }
            if (validateQueryJsonFileExist(queryNum)) {
                queryArrayList.add(queryNum);
            }
        }
    }

    private static boolean validateQueryJsonFileExist(int queryInd) {
        File queryFolder = new File(RESOURCE_FOLDER + separator + "q" + queryInd);
        File queryJson = new File(queryFolder.getAbsolutePath() + separator + "Q" + queryInd + ".json");
        if (queryJson.exists()) {
            return true;
        }
        System.out.println(queryJson.getAbsolutePath() + " does not exists.");
        return false;
    }


    @Test
    public void generateCodeForEachQuery() throws Exception {
        for (int queryIdx : queryArrayList) {
            removeGeneratedCodeFolder(queryIdx);
            generateCodeUsingMainFunction(queryIdx);
            copyGeneratedCodeToQueryFolder(queryIdx);
            removeGeneratedCodeFolder(queryIdx);
        }
        runCommand("which mvn");
    }

    private void generateCodeUsingMainFunction(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File queryJsonFile = new File(queryFolderFile.getAbsolutePath() + separator + "Q" + queryIdx + ".json");

        String flinkInputFile = "file:///aju/q3flinkInput.csv";
        String flinkOutputFile = "file:///aju/q3flinkOutput.csv";

        String[] args = {queryJsonFile.getAbsolutePath(), queryFolderFile.getAbsolutePath(), flinkInputFile, flinkOutputFile, "file"};
        CodeGen.main(args);
    }

    private void generateCodeUsingJar(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File queryJsonFile = new File(queryFolderFile.getAbsolutePath() + separator + "Q" + queryIdx + ".json");

        // TODO
        String flinkInputFile = "file:///aju/q3flinkInput.csv";
        String flinkOutputFile = "file:///aju/q3flinkOutput.csv";

        // repackage the codegen jar
        packageCodegenJar();

        String command_str = "java -jar "
                + CODEGEN_JAR_PATH + " "
                + queryJsonFile.getAbsolutePath() + " "
                + queryFolderFile.getAbsolutePath() + " "
                + flinkInputFile + " "
                + flinkOutputFile + " "
                + "file";
        runCommand(command_str);
    }

    private void runCommand(String command_str) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command_str);
        process.waitFor();

        System.out.println("run command: " + command_str);

        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        String output;
        while ((output = stdInput.readLine()) != null) {
            System.out.println(output);
        }
        while ((output = stdError.readLine()) != null) {
            System.out.println("ERROR:" + output);
        }
    }

    private File getQueryFolderFile(int queryIdx) throws Exception {
        File queryFolderFile = new File(RESOURCE_FOLDER + separator + "q" + queryIdx);
        if (queryFolderFile.exists() && queryFolderFile.isDirectory()) {
            return queryFolderFile;
        } else {
            throw new Exception("q" + queryIdx + " query folder does not exists.");
        }
    }

    private void copyGeneratedCodeToQueryFolder(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File generatedCodeSourceFolder = new File(queryFolderFile.getAbsolutePath() + separator
                + GENERATED_CODE + separator + "src" + separator + "main" + separator
                + "scala" + separator + "org" + separator + "hkust" + separator);
        File[] files = generatedCodeSourceFolder.listFiles();
        for (File file : files) {
            File dstFile = new File(queryFolderFile.getAbsolutePath() + separator + file.getName());
            Files.copy(file.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void removeGeneratedCodeFolder(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File generatedCodeFolder = new File(queryFolderFile.getAbsolutePath() + separator + GENERATED_CODE);
        if (generatedCodeFolder.isDirectory() && generatedCodeFolder.exists()) {
            FileUtils.deleteDirectory(generatedCodeFolder);
        }
    }

    private void packageCodegenJar() throws IOException, InterruptedException {
        runCommand("mvn package -DskipTests -f .");
    }

}
