import org.ainslec.picocog.PicoWriter;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class MainClassWriter implements ClassWriter {
    private static final String CLASS_NAME = "Job";
    private final Node query;
    private final PicoWriter writer = new PicoWriter();

    public MainClassWriter(Node query) {
        requireNonNull(query);
        this.query = query;
    }

    @Override
    public void generateCode(String filePath) throws IOException {
        addImports();
        addConstructorAndOpenClass();
        addMainFunction();
        addGetStreamFunction();
        closeClass(writer);
        writeClassFile(CLASS_NAME, filePath, writer.toString());
    }

    @Override
    public void addImports() {
        writer.writeln("import org.apache.flink.api.java.utils.ParameterTool");
        writer.writeln("import org.apache.flink.core.fs.FileSystem");
        writer.writeln("import org.apache.flink.streaming.api.TimeCharacteristic");
        writer.writeln("import org.apache.flink.streaming.api.scala._");
        writer.writeln("import org.hkust.ProcessFunction.Q6.{" + query.getAggregateProcessFunction().getName() + ", " + query.getRelationProcessFunction().getName() + "}");
        writer.writeln("import org.hkust.RelationType.Payload");
    }

    @Override
    public void addConstructorAndOpenClass() {
        writer.writeln_r(CLASS_NAME + " {");
    }

    private void addMainFunction() {
        writer.writeln_r("def main(args: Array[String]) {");
        writer.writeln("val env = StreamExecutionEnvironment.getExecutionEnvironment");
        writer.writeln("val params: ParameterTool = ParameterTool.fromArgs(args)");
        writer.writeln("env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)");
        writer.writeln("var executionConfig = env.getConfig");
        writer.writeln("executionConfig.enableObjectReuse()");
        writer.writeln("val inputpath = " + query.getConfiguration().getInputPath());
        writer.writeln("val outputpath = " + query.getConfiguration().getOutputPath());
        writer.writeln("val inputStream : DataStream[Payload] = getStream(env,inputpath)");
        writer.writeln("val result  = inputStream.keyBy(i => i._3)");
        writer.writeln(".process(new " + ProcessFunctionWriter.makeClassName(query.getRelationProcessFunction().getName()) + "())");
        writer.writeln(".keyBy(i => i._3)");
        writer.writeln(".process(new " + ProcessFunctionWriter.makeClassName(query.getAggregateProcessFunction().getName()) + ")");
        writer.writeln(".map(x => (x._4.mkString(\", \"), x._5.mkString(\", \")))");
        writer.writeln(".writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE)");
        writer.writeln(".setParallelism(1)");
        writer.writeln("env.execute(\"Flink Streaming Scala API Skeleton\")");
        writer.writeln_l("}");
    }

    private void addGetStreamFunction() {
        writer.writeln_r("private def getStream(env: StreamExecutionEnvironment, dataPath: String): DataStream[Payload] = {");
        writer.writeln("val data = env.readTextFile(dataPath).setParallelism(1)");
        writer.writeln("val format = new java.text.SimpleDateFormat(\"yyyy-MM-dd\")");
        writer.writeln("val restDS : DataStream[Payload] = data");
        writer.writeln(".map(line => {");
        writer.writeln("val header = line.substring(0,3)");
        writer.writeln("val cells : Array[String] = line.substring(3).split(\"\\|\")");
        writer.writeln("var cnt : Long = 0");
        writer.writeln("val i = Tuple4(cells(4).toDouble, cells(5).toDouble, cells(6).toDouble, format.parse(cells(10)))");
        writer.writeln("var relation = \"\"");
        writer.writeln("var action = \"\"");
        writer.writeln("header match {");
        writer.writeln("case \"+LI\" =>");
        writer.writeln("relation = \"Lineitem\"");
        writer.writeln("action = \"Insert\"");
        writer.writeln("case \"-LI\" =>");
        writer.writeln("relation = \"Lineitem\"");
        writer.writeln("action = \"Delete\"");
        writer.writeln("}");
        writer.writeln("cnt = cnt + 1");
        writer.writeln("Payload(relation, action,");
        writer.writeln("Tuple2(cells(0).toInt, cells(3).toInt).asInstanceOf[Any],");
        writer.writeln("Array(i._1, i._2, i._3, i._4),");
        writer.writeln("Array(\"QUANTITY\", \"EXTENDEDPRICE\", \"DISCOUNT\", \"SHIPDATE\"), cnt)");
        writer.writeln("}).setParallelism(1).filter(x => x._1 != \"\").setParallelism(1)");
        writer.writeln("restDS");
        writer.writeln_l("}");
    }
}