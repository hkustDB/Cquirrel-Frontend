import org.ainslec.picocog.PicoWriter;

import java.io.IOException;

public class MainClassWriter implements ClassWriter {
    private static final String CLASS_NAME = "Job";
    private final String aggregateProcessFunctionName;
    private final String relationProcessFunctionName;

    private final static PicoWriter writer = new PicoWriter();

    public MainClassWriter(String aggregateProcessFunctionName, String relationProcessFunctionName) {
        CheckerUtils.checkNullOrEmpty(aggregateProcessFunctionName, "aggregateProcessFunctionName");
        CheckerUtils.checkNullOrEmpty(relationProcessFunctionName, "relationProcessFunctionName");
        this.aggregateProcessFunctionName = aggregateProcessFunctionName;
        this.relationProcessFunctionName = relationProcessFunctionName;
    }

    @Override
    public void generateCode(String filePath) throws IOException {
        addImports();
        addConstructorAndOpenClass();
        addMainFunction();
        closeClass(writer);
        writeClassFile(CLASS_NAME, filePath, writer.toString());

    }

    @Override
    public void addImports() {
        writer.writeln("import org.apache.flink.api.java.utils.ParameterTool");
        writer.writeln("import org.apache.flink.core.fs.FileSystem");
        writer.writeln("import org.apache.flink.streaming.api.TimeCharacteristic");
        writer.writeln("import org.apache.flink.streaming.api.scala._");
        writer.writeln("import org.hkust.ProcessFunction.Q6.{" + aggregateProcessFunctionName + ", " + relationProcessFunctionName + "}");
        writer.writeln("import org.hkust.RelationType.Payload");
    }

    @Override
    public void addConstructorAndOpenClass() {
        writer.writeln_r(CLASS_NAME + " {");
    }

    private void addMainFunction() {

    }

}