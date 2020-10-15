import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class CodeGenerator {
    private static final String GENERATED_CODE = "generated-code";

    public static void generate(String jsonFilePath, String flinkInputPath, String jarOutputPath, String flinkOutputPath) throws Exception {
        CheckerUtils.checkNullOrEmpty(jsonFilePath, "jsonFilePath");
        CheckerUtils.checkNullOrEmpty(jarOutputPath, "jarOutputPath");
        CheckerUtils.checkNullOrEmpty(flinkInputPath, "flinkInputPath");
        CheckerUtils.checkNullOrEmpty(flinkOutputPath, "flinkOutputPath");

        Node node = JsonParser.parse(jsonFilePath);

        String rpfClassName = new RelationProcessFunctionWriter(node.getRelationProcessFunction()).write(jarOutputPath);
        copyToGeneratedCode(jarOutputPath, rpfClassName);

        String agpClassName = new AggregateProcessFunctionWriter(node.getAggregateProcessFunction()).write(jarOutputPath);
        copyToGeneratedCode(jarOutputPath, agpClassName);

        String mainClassName = new MainClassWriter(node, flinkInputPath, flinkOutputPath).write(jarOutputPath);
        copyToGeneratedCode(jarOutputPath, mainClassName);

        compile(jarOutputPath + File.separator + GENERATED_CODE + File.separator + "pom.xml");

    }

    private static void copyToGeneratedCode(String outputPath, String className) throws IOException {
        Files.copy(
                Paths.get(getClassFilePath(outputPath, className)),
                Paths.get(outputPath + File.separator + GENERATED_CODE + File.separator + "src"+ File.separator +"main"+ File.separator +"scala"+ File.separator +"org"+ File.separator +"hkust" + File.separator + getClassFileName(className)),
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private static void compile(String pomPath) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        execute(runtime, "mvn install -f " + pomPath);
        execute(runtime, "mvn compile -f " + pomPath);
        execute(runtime, "mvn package -f " + pomPath);
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

    private static String getClassFilePath(String outputPath, String className) {
        return outputPath + File.separator + getClassFileName(className);
    }

    private static String getClassFileName(String name) {
        return name + ".scala";
    }
}
