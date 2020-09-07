import java.io.File;

public class CodeGen {
    private int NUM_OF_ARGS = 2;

    public void main(String[] args) {
        System.out.println("\n" +
                "   _     __                                 _        ___           \n" +
                "  /_\\    \\ \\  /\\ /\\            ___ ___   __| | ___  / _ \\___ _ __  \n" +
                " //_\\\\    \\ \\/ / \\ \\  _____   / __/ _ \\ / _` |/ _ \\/ /_\\/ _ \\ '_ \\ \n" +
                "/  _  \\/\\_/ /\\ \\_/ / |_____| | (_| (_) | (_| |  __/ /_\\\\  __/ | | |\n" +
                "\\_/ \\_/\\___/  \\___/           \\___\\___/ \\__,_|\\___\\____/\\___|_| |_|\n" +
                "                                                                   \n");
        validateArgs(args);
    }

    private void validateArgs(String[] args) {
        if (args.length != NUM_OF_ARGS) {
            throw new RuntimeException("Expecting exactly 2 input strings: JSON file path and jar output path");
        }

        validateJsonFile(args[0]);
        validateOutputDir(args[1]);
    }

    private void validateJsonFile(String jsonFilePath) {
        CheckerUtils.checkNullOrEmpty(jsonFilePath, "jsonFilePath");
        if (jsonFilePath.endsWith(".json")) {
            if (new File(jsonFilePath).exists()) return;
            throw new RuntimeException("Unable to find JSON file");
        } else {
            throw new RuntimeException("Path provided isn't for a .json file: " + jsonFilePath);
        }
    }

    private void validateOutputDir(String outputDirPath) {
        CheckerUtils.checkNullOrEmpty(outputDirPath, "outputDirPath");
        File outputDir = new File(outputDirPath);
        if (outputDir.exists() && outputDir.isDirectory()) return;

        throw new RuntimeException("output directory must exist and must be a directory, got: " + outputDirPath);
    }
}
