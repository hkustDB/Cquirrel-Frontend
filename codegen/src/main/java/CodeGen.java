import com.google.common.collect.ImmutableSet;

import java.io.File;
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

        CodeGenerator.generate(args[JSON_FILE_INDEX], args[GENERATED_JAR_INDEX], args[INPUT_PATH_INDEX], args[OUTPUT_PATH_INDEX]);
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
