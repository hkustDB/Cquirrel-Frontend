import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Before;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

public class GenerateCodeTest {
    private static ArrayList<Integer> queryArrayList = null;

    private static final String RESOURCE_FOLDER = new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath();
    private final String CODEGEN_JAR_PATH = new File(CodeGen.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile().getAbsolutePath() + File.separator + "codegen-1.0-SNAPSHOT.jar";

    @BeforeClass
    public static void getQueryArrayList() {
        // check if resource folder exists and is a directory
        File resourceFolderFile = new File(RESOURCE_FOLDER);
        assertTrue(resourceFolderFile.exists());
        assertTrue(resourceFolderFile.isDirectory());

        initQueryArrayList();

        // check the child folder of the resource folder
        File[] resourceFolderFiles = resourceFolderFile.listFiles();
        for (File file : resourceFolderFiles) {
            if (!file.isDirectory()) {
                continue;
            }
            if (file.getName().charAt(0) != 'q') {
                continue;
            }
            int queryNum;
            try {
                queryNum = Integer.parseInt(file.getName().substring(1));
            } catch (NumberFormatException e) {
                System.out.println(file.getName() + " is not a query name.");
                continue;
            }
            // TPC-H has only 1~22 queries.
            if (queryNum < 1 || queryNum > 22) {
                System.out.println("TPC-H does not contain query " + String.valueOf(queryNum) + ".");
                continue;
            }
            if (validateQueryJsonFileExist(queryNum)) {
                queryArrayList.add(queryNum);
            }
        }
    }

    /*
     * init the queryArrayList
     */
    private static void initQueryArrayList() {
        if (queryArrayList != null) {
            queryArrayList.clear();
        } else {
            queryArrayList = new ArrayList<>();
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

        String output = null;
        while ((output = stdInput.readLine()) != null) {
            System.out.println(output);
        }
        while ((output = stdError.readLine()) != null) {
            System.out.println("ERROR:" + output);
        }
    }

    private void copyGeneratedCodeToQueryFolder(int queryIdx) throws IOException {
        File queryFolderFile = new File(RESOURCE_FOLDER + File.separator + "q" + String.valueOf(queryIdx));
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
        File queryFolderFile = new File(RESOURCE_FOLDER + File.separator + "q" + String.valueOf(queryIdx));
        File generatedCodeFolder = new File(queryFolderFile.getAbsolutePath() + File.separator + "generated-code");
        if (generatedCodeFolder.isDirectory() && generatedCodeFolder.exists()) {
            deleteFolder(generatedCodeFolder);
        }
    }

    private static void deleteFolder(File folder) throws Exception {
        if (!folder.exists()) {
            throw new Exception(folder.getAbsolutePath() + " does not exists.");
        }
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    file.delete();
                }
            }
        }
        folder.delete();
    }
}
