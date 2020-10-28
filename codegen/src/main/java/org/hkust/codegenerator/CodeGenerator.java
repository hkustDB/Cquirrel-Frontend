package org.hkust.codegenerator;

import org.hkust.checkerutils.CheckerUtils;
import org.hkust.jsonutils.JsonParser;
import org.hkust.jsonutils.Node;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class CodeGenerator {
    public static final String GENERATED_CODE = "generated-code";

    public static void generate(String jsonFilePath, String jarOutputPath, String flinkInputPath, String flinkOutputPath) throws Exception {
        CheckerUtils.checkNullOrEmpty(jsonFilePath, "jsonFilePath");
        CheckerUtils.checkNullOrEmpty(jarOutputPath, "jarOutputPath");
        CheckerUtils.checkNullOrEmpty(flinkInputPath, "flinkInputPath");
        CheckerUtils.checkNullOrEmpty(flinkOutputPath, "flinkOutputPath");

        Node node = JsonParser.parse(jsonFilePath);

        String codeFilesPath = jarOutputPath + File.separator + GENERATED_CODE + File.separator + "src" + File.separator + "main" + File.separator + "scala" + File.separator + "org" + File.separator + "hkust";
        new RelationProcessFunctionWriter(node.getRelationProcessFunction()).write(codeFilesPath);

        new AggregateProcessFunctionWriter(node.getAggregateProcessFunction()).write(codeFilesPath);

        new MainClassWriter(node, flinkInputPath, flinkOutputPath).write(codeFilesPath);

        compile(jarOutputPath + File.separator + GENERATED_CODE + File.separator + "pom.xml");

    }

    private static void compile(String pomPath) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        //TODO: do we need to do all 3? Wouldn't package alone be sufficient?
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
