import com.google.common.collect.ImmutableSet;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;

public class CodeGen {
    private static int NUM_OF_ARGS = 5;
    private static final Set<String> IO_TYPES = ImmutableSet.of("file", "kafka");
    private static final int JSON_FILE_INDEX = 0;
    private static final int GENERATED_JAR_INDEX = 1;
    private static final int OUTPUT_PATH_INDEX = 2;
    private static final int INPUT_PATH_INDEX = 3;
    private static final int IO_TYPE_INDEX = 4;

    public static void main(String[] args) throws Exception {
        System.out.println("\n" +
                "   _     __                                 _        ___           \n" +
                "  /_\\    \\ \\  /\\ /\\            ___ ___   __| | ___  / _ \\___ _ __  \n" +
                " //_\\\\    \\ \\/ / \\ \\  _____   / __/ _ \\ / _` |/ _ \\/ /_\\/ _ \\ '_ \\ \n" +
                "/  _  \\/\\_/ /\\ \\_/ / |_____| | (_| (_) | (_| |  __/ /_\\\\  __/ | | |\n" +
                "\\_/ \\_/\\___/  \\___/           \\___\\___/ \\__,_|\\___\\____/\\___|_| |_|\n" +
                "                                                                   \n");
        validateArgs(args);
        prepareEnvironment(args[GENERATED_JAR_INDEX]);
        CodeGenerator.generate(args[JSON_FILE_INDEX], args[GENERATED_JAR_INDEX], args[INPUT_PATH_INDEX], args[OUTPUT_PATH_INDEX]);
    }

    private static void prepareEnvironment(String jarPath) throws IOException, URISyntaxException {
        StringBuilder generatedCodeBuilder = new StringBuilder();
        String generatedCode = "generated-code";
        generatedCodeBuilder.append(jarPath)
                .append(File.separator)
                .append(generatedCode)
                .append(File.separator)
                .append("src")
                .append(File.separator)
                .append("main")
                .append(File.separator)
                .append("scala")
                .append(File.separator)
                .append("org")
                .append(File.separator)
                .append("hkust");

        File directory = new File(generatedCodeBuilder.toString());
        if (!directory.exists()) {
            directory.mkdirs();
        }
        extractPomFile(jarPath + File.separator + generatedCode);
    }

    private static void extractPomFile(String path) throws IOException, URISyntaxException {
        String pomName = "pom.xml";
        File target = new File(path + File.separator + pomName);
        if (target.exists())
            return;

        FileOutputStream out = new FileOutputStream(target);
        InputStream in = new FileInputStream(new File(CodeGen.class.getResource(pomName).toURI()));

        byte[] buf = new byte[8 * 1024];
        int len;
        while ((len = in.read(buf)) != -1) {
            out.write(buf, 0, len);
        }
        out.close();
        in.close();
    }

    private static void validateArgs(String[] args) {
        if (args.length != NUM_OF_ARGS) {
            throw new RuntimeException("Expecting exactly " + NUM_OF_ARGS + " input strings: JSON file path, generated-code jar output path, flink input path, flink output path and flink I/O type");
        }

        validateJsonFile(args[JSON_FILE_INDEX]);
        validateDirectoryPath(args[GENERATED_JAR_INDEX]);
        validateDirectoryPath(args[OUTPUT_PATH_INDEX]);
        validateDirectoryPath(args[INPUT_PATH_INDEX]);
        validateFlinkIOType(args[IO_TYPE_INDEX]);
    }

    private static void validateJsonFile(String jsonFilePath) {
        CheckerUtils.checkNullOrEmpty(jsonFilePath, "jsonFilePath");
        if (jsonFilePath.endsWith(".json")) {
            if (new File(jsonFilePath).exists()) return;
            throw new RuntimeException("Unable to find JSON file");
        } else {
            throw new RuntimeException("Path provided isn't for a .json file: " + jsonFilePath);
        }
    }

    private static void validateDirectoryPath(String directoryPath) {
        CheckerUtils.checkNullOrEmpty(directoryPath, "directoryPath");
        File outputDir = new File(directoryPath);
        if (outputDir.exists() && outputDir.isDirectory()) return;

        throw new RuntimeException("output directory must exist and must be a directory, got: " + directoryPath);
    }

    private static void validateFlinkIOType(String ioType) {
        CheckerUtils.checkNullOrEmpty(ioType, "ioType");
        if (!IO_TYPES.contains(ioType.toLowerCase())) {
            throw new RuntimeException("Only the following flink IO types are supported: " + Arrays.toString(IO_TYPES.toArray()));
        }
    }
}
