import java.io.File;

public class CodeGen {
    private int NUM_OF_ARGS = 2;

    public void main(String[] args) {
        System.out.println("AJU - CodeGen");
        validateArgs(args);
    }

    private void validateArgs(String[] args) {
        if (args.length != NUM_OF_ARGS) {
            throw new RuntimeException("Expecting exactly 2 input strings: JSON file path and flink task manager address");
        }
        for (String arg : args) {
            if (arg == null || arg.isEmpty()) {
                throw new RuntimeException("Argument cannot be null or empty");
            }
        }

        File jsonFile = new File(args[0]);
        if (!jsonFile.exists()) {
            throw new RuntimeException("Unable to find JSON file");
        }
    }
}
