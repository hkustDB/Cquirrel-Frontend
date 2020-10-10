import scala.collection.JavaConverters;
import scala.tools.nsc.Global;
import scala.tools.nsc.Settings;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class CodeGenerator {
    public static void generate(Node node, String outputPath) throws IOException {
        requireNonNull(node);
        CheckerUtils.checkNullOrEmpty(outputPath, "outputPath");
        new RelationProcessFunctionWriter(node.getRelationProcessFunction()).generateCode(outputPath);
        new AggregateProcessFunctionWriter(node.getAggregateProcessFunction()).generateCode(outputPath);
        String mainClassName = new MainClassWriter(node).generateCode(outputPath);

        compile(outputPath + File.separator + mainClassName);
    }

    private static void compile(String filePath) {
        Global g = new Global(new Settings());
        Global.Run run = g.new Run();
        List<String> fileNames = new ArrayList<>(singletonList(filePath));

        run.compile(JavaConverters.asScalaBuffer(fileNames).toList());
    }
}
