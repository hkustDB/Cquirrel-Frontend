import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.commons.io.FileUtils;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;


public class GenerateCodeTest {
    private static ArrayList<Integer> queryArrayList = new ArrayList<>();

    private static final String RESOURCE_FOLDER = new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath();
    private final String CODEGEN_JAR_PATH = new File(CodeGen.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile().getAbsolutePath() + File.separator + "codegen-1.0-SNAPSHOT.jar";

    @BeforeClass
    public static void getQueryArrayList() throws Exception {
        // check if resource folder exists and is a directory
        File resourceFolderFile = new File(RESOURCE_FOLDER);
        if(!(resourceFolderFile.exists() && resourceFolderFile.isDirectory())) {
            throw new Exception("resource folder does not exists.");
        }

        // check the child folder of the resource folder
        File[] resourceFolderFiles = resourceFolderFile.listFiles();
        for (File file : resourceFolderFiles) {
            // jump the non-query folder
            if ((!file.isDirectory()) || (file.getName().charAt(0) != 'q')) {
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

    /*
     * check if the json file exists under the query folders
     */
    private static boolean validateQueryJsonFileExist(int queryInd) {
        File queryFolderFile = new File(RESOURCE_FOLDER + File.separator + "q" + String.valueOf(queryInd));
        File queryJsonFile = new File(queryFolderFile.getAbsolutePath() + File.separator + "Q" + String.valueOf(queryInd) + ".json");
        if (queryJsonFile.exists()) {
            return true;
        }
        System.out.println(queryJsonFile.getAbsolutePath() + " does not exists.");
        return false;
    }


    @Test
    public void generateCodeForEachQuery() throws Exception {
        for (int queryIdx : queryArrayList) {
            removeGeneratedCodeFolder(queryIdx);
            generateCode(queryIdx);
            copyGeneratedCodeToQueryFolder(queryIdx);
            removeGeneratedCodeFolder(queryIdx);
        }
    }

    private void generateCode(int queryIdx) throws IOException, InterruptedException {
        File queryFolderFile = new File(RESOURCE_FOLDER + File.separator + "q" + String.valueOf(queryIdx));
        File queryJsonFile = new File(queryFolderFile.getAbsolutePath() + File.separator + "Q" + String.valueOf(queryIdx) + ".json");

        // TODO
        String flinkInputFile = "file:///aju/q3flinkInput.csv";
        String flinkOutputFile = "file:///aju/q3flinkOutput.csv";

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
        File queryFolderFile = new File(RESOURCE_FOLDER + File.separator + "q" + queryIdx);
        if (queryFolderFile.exists() && queryFolderFile.isDirectory()) {
            return queryFolderFile;
        } else {
            throw new Exception("q"+ queryIdx + " query folder does not exists.");
        }
    }

    private void copyGeneratedCodeToQueryFolder(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File generatedCodeSourceFolder = new File(queryFolderFile.getAbsolutePath() + File.separator
                + "generated-code" + File.separator + "src" + File.separator + "main" + File.separator
                + "scala" + File.separator + "org" + File.separator + "hkust" + File.separator);
        File[] files = generatedCodeSourceFolder.listFiles();
        for (File file : files) {
            File dstFile = new File(queryFolderFile.getAbsolutePath() + File.separator + file.getName());
            if (dstFile.exists()) {
                dstFile.delete();
            }
            Files.copy(file.toPath(), dstFile.toPath());
        }
    }

    private void removeGeneratedCodeFolder(int queryIdx) throws Exception {
        File queryFolderFile = getQueryFolderFile(queryIdx);
        File generatedCodeFolder = new File(queryFolderFile.getAbsolutePath() + File.separator + "generated-code");
        if (generatedCodeFolder.isDirectory() && generatedCodeFolder.exists()) {
            FileUtils.deleteDirectory(generatedCodeFolder);
        }
    }

}
