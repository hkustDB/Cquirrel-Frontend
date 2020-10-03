public class Configuration {
    private final String inputPath;
    private final String outputPath;

    public Configuration(String inputPath, String outputPath) {
        CheckerUtils.checkNullOrEmpty(inputPath,"inputPath");
        CheckerUtils.checkNullOrEmpty(outputPath,"outputPath");
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "inputPath='" + inputPath + '\'' +
                ", outputPath='" + outputPath + '\'' +
                '}';
    }
}
