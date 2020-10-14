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

    static void extractPomFile(String path) throws IOException {
        String pomName = "pom.xml";
        String content = "<project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                "         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd\">\n" +
                "    <modelVersion>4.0.0</modelVersion>\n" +
                "    <groupId>org.hkust</groupId>\n" +
                "    <artifactId>generated-code</artifactId>\n" +
                "    <version>1.0-SNAPSHOT</version>\n" +
                "    <name>${project.artifactId}</name>\n" +
                "\n" +
                "    <properties>\n" +
                "        <maven.compiler.source>1.8</maven.compiler.source>\n" +
                "        <maven.compiler.target>1.8</maven.compiler.target>\n" +
                "        <encoding>UTF-8</encoding>\n" +
                "        <scala.version>2.12.6</scala.version>\n" +
                "        <scala.compat.version>2.12</scala.compat.version>\n" +
                "        <spec2.version>4.2.0</spec2.version>\n" +
                "    </properties>\n" +
                "\n" +
                "    <dependencies>\n" +
                "        <dependency>\n" +
                "            <groupId>org.hkust</groupId>\n" +
                "            <artifactId>AJU</artifactId>\n" +
                "            <version>1.0-SNAPSHOT</version>\n" +
                "        </dependency>\n" +
                "        <dependency>\n" +
                "            <groupId>org.apache.flink</groupId>\n" +
                "            <artifactId>flink-streaming-scala_2.12</artifactId>\n" +
                "            <version>1.11.2</version>\n" +
                "            <scope>provided</scope>\n" +
                "        </dependency>\n" +
                "    </dependencies>\n" +
                "\n" +
                "    <build>\n" +
                "        <sourceDirectory>src/main/scala</sourceDirectory>\n" +
                "        <plugins>\n" +
                "            <plugin>\n" +
                "                <!-- see http://davidb.github.com/scala-maven-plugin -->\n" +
                "                <groupId>net.alchim31.maven</groupId>\n" +
                "                <artifactId>scala-maven-plugin</artifactId>\n" +
                "                <version>3.3.2</version>\n" +
                "                <executions>\n" +
                "                    <execution>\n" +
                "                        <goals>\n" +
                "                            <goal>compile</goal>\n" +
                "                        </goals>\n" +
                "                    </execution>\n" +
                "                </executions>\n" +
                "            </plugin>\n" +
                "            <plugin>\n" +
                "                <groupId>org.apache.maven.plugins</groupId>\n" +
                "                <artifactId>maven-surefire-plugin</artifactId>\n" +
                "                <version>2.21.0</version>\n" +
                "                <configuration>\n" +
                "                    <!-- Tests will be run with scalatest-maven-plugin instead -->\n" +
                "                    <skipTests>true</skipTests>\n" +
                "                </configuration>\n" +
                "            </plugin>\n" +
                "            <plugin>\n" +
                "                <groupId>org.apache.maven.plugins</groupId>\n" +
                "                <artifactId>maven-assembly-plugin</artifactId>\n" +
                "                <version>2.4</version>\n" +
                "                <configuration>\n" +
                "                    <descriptorRefs>\n" +
                "                        <descriptorRef>jar-with-dependencies</descriptorRef>\n" +
                "                    </descriptorRefs>\n" +
                "                    <archive>\n" +
                "                        <manifest>\n" +
                "                            <mainClass>Job</mainClass>\n" +
                "                        </manifest>\n" +
                "                    </archive>\n" +
                "                </configuration>\n" +
                "                <executions>\n" +
                "                    <execution>\n" +
                "                        <phase>package</phase>\n" +
                "                        <goals>\n" +
                "                            <goal>single</goal>\n" +
                "                        </goals>\n" +
                "                    </execution>\n" +
                "                </executions>\n" +
                "            </plugin>\n" +
                "        </plugins>\n" +
                "    </build>\n" +
                "</project>\n";
        File target = new File(path + File.separator + pomName);

        FileOutputStream outputStream = new FileOutputStream(target);
        byte[] bytes = content.getBytes();
        outputStream.write(bytes);
        outputStream.close();
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
