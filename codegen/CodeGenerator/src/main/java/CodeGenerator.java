import scala.collection.JavaConversions;
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
        new RelationProcessFunctionWriter(node.getRelationProcessFunction()).write(outputPath);
        new AggregateProcessFunctionWriter(node.getAggregateProcessFunction()).write(outputPath);
        String mainClassName = new MainClassWriter(node).write(outputPath);

        compile(outputPath + File.separator + mainClassName + ".scala");
    }

    private static void compile(String filePath) {
        Global global = new Global(new Settings());
        Global.Run run = global.new Run();
        List<String> fileNames = new ArrayList<>(singletonList(filePath));
        run.compile(JavaConversions.asScalaBuffer(fileNames).toList());
    }
}
