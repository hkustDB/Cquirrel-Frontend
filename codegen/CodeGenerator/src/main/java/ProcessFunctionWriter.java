import org.ainslec.picocog.PicoWriter;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;

public abstract class ProcessFunctionWriter {
    public abstract void generateCode(final String filePath) throws IOException;

    abstract void addImports();

    abstract void addConstructorAndOpenClass();

    void closeClass(final PicoWriter writer) {
        writer.writeln_r("}");
    }

    String keyListToCode(@Nullable List<String> keyList) {
        StringBuilder code = new StringBuilder();
        code.append("Array(");
        if (keyList != null) {
            for (int i = 0; i < keyList.size(); i++) {
                code.append("\"");
                code.append(keyList.get(i));
                code.append("\"");
                if (i != keyList.size() - 1) {
                    code.append(",");
                }
            }
        }
        code.append(")");

        return code.toString();
    }
}
