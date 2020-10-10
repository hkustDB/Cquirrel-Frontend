import org.ainslec.picocog.PicoWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public interface ClassWriter {
    String generateCode(final String filePath) throws IOException;
    void addImports();

    void addConstructorAndOpenClass();

    default void closeClass(final PicoWriter writer) {
        writer.writeln_r("}");
    }

    default void writeClassFile(final String className, final String path, final String code) throws IOException {
        CheckerUtils.checkNullOrEmpty(className, "className");
        CheckerUtils.checkNullOrEmpty(path, "path");
        CheckerUtils.checkNullOrEmpty(code, "code");
        Files.write(Paths.get(path + File.separator + className + ".scala"), code.getBytes());
    }

    default String makeClassName(String name) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        return name + "ProcessFunction";
    }
}
