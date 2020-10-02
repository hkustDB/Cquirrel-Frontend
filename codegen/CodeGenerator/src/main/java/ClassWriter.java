import org.ainslec.picocog.PicoWriter;

import java.io.IOException;

public interface ClassWriter {
    public void generateCode(final String filePath) throws IOException;
    void addImports();

    void addConstructorAndOpenClass();

    default void closeClass(final PicoWriter writer) {
        writer.writeln_r("}");
    }
}
