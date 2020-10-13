import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static java.util.Objects.requireNonNull;

public class CodeGenerator {
    private static final String GENERATED_CODE = "generated-code";

    public static void generate(Node node, String outputPath) throws IOException {
        requireNonNull(node);
        CheckerUtils.checkNullOrEmpty(outputPath, "outputPath");

        String rpfClassName = new RelationProcessFunctionWriter(node.getRelationProcessFunction()).write(outputPath);
        copyToGeneratedCode(outputPath, rpfClassName);

        String agpClassName = new AggregateProcessFunctionWriter(node.getAggregateProcessFunction()).write(outputPath);
        copyToGeneratedCode(outputPath, agpClassName);

        String mainClassName = new MainClassWriter(node).write(outputPath);
        copyToGeneratedCode(outputPath, mainClassName);

        compile(outputPath + File.separator + GENERATED_CODE + File.separator + "pom.xml");

    }

    private static void copyToGeneratedCode(String outputPath, String className) throws IOException {
        Files.copy(
                Paths.get(getClassFilePath(outputPath, className)),
                Paths.get(outputPath + File.separator + GENERATED_CODE + "/src/main/scala/org/hkust" + File.separator + getClassFileName(className)),
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
