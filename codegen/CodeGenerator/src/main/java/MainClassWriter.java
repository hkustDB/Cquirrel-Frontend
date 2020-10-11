import org.ainslec.picocog.PicoWriter;

import java.io.IOException;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class MainClassWriter implements ClassWriter {
    private static final String CLASS_NAME = "Job";
    private final AggregateProcessFunction aggregateProcessFunction;
    private final String aggregateProcFuncClassName;
    private final RelationProcessFunction relationProcessFunction;
    private final String relationProcFuncClassName;
    private final Configuration configuration;
    private final PicoWriter writer = new PicoWriter();

    private static final Map<Class, String> stringConversionMethods = new HashMap<>();

    static {
        stringConversionMethods.put(Integer.class, "toInt");
        stringConversionMethods.put(Double.class, "toDouble");
        stringConversionMethods.put(Long.class, "toLong");
        stringConversionMethods.put(Date.class, "format.parse");
    }

    public MainClassWriter(Node query) {
        requireNonNull(query);
        this.aggregateProcessFunction = query.getAggregateProcessFunction();
        this.aggregateProcFuncClassName = getProcessFunctionClassName(aggregateProcessFunction.getName());
        this.relationProcessFunction = query.getRelationProcessFunction();
        this.relationProcFuncClassName = getProcessFunctionClassName(relationProcessFunction.getName());
        this.configuration = query.getConfiguration();
    }

    @Override
    public String write(String filePath) throws IOException {
        addImports();
        addConstructorAndOpenClass();
        addMainFunction();
        addGetStreamFunction();
        closeClass(writer);
        writeClassFile(CLASS_NAME, filePath, writer.toString());

        return CLASS_NAME;
    }

    @Override
    public void addImports() {
        writer.writeln("import org.apache.flink.api.java.utils.ParameterTool");
        writer.writeln("import org.apache.flink.core.fs.FileSystem");
        writer.writeln("import org.apache.flink.streaming.api.TimeCharacteristic");
        writer.writeln("import org.apache.flink.streaming.api.scala._");
        writer.writeln("import org.hkust.ProcessFunction.Q6.{" + getProcessFunctionClassName(aggregateProcessFunction.getName()) + ", " + getProcessFunctionClassName(relationProcessFunction.getName()) + "}");
        writer.writeln("import org.hkust.RelationType.Payload");
    }

    @Override
    public void addConstructorAndOpenClass() {
        writer.writeln_r("object " + CLASS_NAME + " {");
    }

    private void addMainFunction() {
        writer.writeln_r("def main(args: Array[String]) {");
        writer.writeln("val env = StreamExecutionEnvironment.getExecutionEnvironment");
        writer.writeln("val params: ParameterTool = ParameterTool.fromArgs(args)");
        writer.writeln("env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)");
        writer.writeln("var executionConfig = env.getConfig");
        writer.writeln("executionConfig.enableObjectReuse()");
        writer.writeln("val inputpath = \"" + configuration.getInputPath() + "\"");
        writer.writeln("val outputpath = \"" + configuration.getOutputPath() + "\"");
        writer.writeln("val inputStream : DataStream[Payload] = getStream(env,inputpath)");
        writer.writeln("val result  = inputStream.keyBy(i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(relationProcessFunction.getName()) + "())");
        writer.writeln(".keyBy(i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunction.getName()) + ")");
        writer.writeln(".map(x => (x._4.mkString(\", \"), x._5.mkString(\", \")))");
        writer.writeln(".writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE)");
        writer.writeln(".setParallelism(1)");
        writer.writeln("env.execute(\"Flink Streaming Scala API Skeleton\")");
        writer.writeln_l("}");
    }

    private void addGetStreamFunction() {
        Set<RelationSchema.Attribute> attributes = extractAttributes();
        StringBuilder columnNamesCode = new StringBuilder();
        StringBuilder tupleCode = new StringBuilder();
        attributeCode(attributes, columnNamesCode, tupleCode);
        String lowerRelationName = relationProcessFunction.getRelationName().toLowerCase();
        String formattedRelationName = lowerRelationName.substring(0, 1).toUpperCase() + lowerRelationName.substring(1);
        writer.writeln_r("private def getStream(env: StreamExecutionEnvironment, dataPath: String): DataStream[Payload] = {");
        writer.writeln("val data = env.readTextFile(dataPath).setParallelism(1)");
        writer.writeln("val format = new java.text.SimpleDateFormat(\"yyyy-MM-dd\")");
        writer.writeln("var cnt : Long = 0");
        writer.writeln("val restDS : DataStream[Payload] = data");
        writer.writeln(".map(line => {");
        writer.writeln("val header = line.substring(0,3)");
        writer.writeln("val cells : Array[String] = line.substring(3).split(\"\\\\|\")");
        writer.writeln("val i = Tuple" + attributes.size() + "(" + tupleCode.toString() + ")");
        writer.writeln("var relation = \"\"");
        writer.writeln("var action = \"\"");
        writer.writeln("header match {");
        writer.writeln("case \"+LI\" =>");
        writer.writeln("relation = \"" + formattedRelationName + "\"");
        writer.writeln("action = \"Insert\"");
        writer.writeln("case \"-LI\" =>");
        writer.writeln("relation = \"" + formattedRelationName + "\"");
        writer.writeln("action = \"Delete\"");
        writer.writeln("}");
        writer.writeln("cnt = cnt + 1");
        writer.writeln("Payload(relation, action,");
        writer.writeln("Tuple2(cells(0).toInt, cells(3).toInt).asInstanceOf[Any],");
        writer.writeln("Array(" + iteratorCode(attributes.size()) + "),");
        writer.writeln("Array(" + columnNamesCode.toString() + "), cnt)");
        writer.writeln("}).setParallelism(1).filter(x => x._1 != \"\").setParallelism(1)");
        writer.writeln("restDS");
        writer.writeln_l("}");
    }

    private String iteratorCode(int num) {
        StringBuilder code = new StringBuilder();
        num++;
        for (int i = 1; i < num; i++) {
            code.append("i._").append(i);
            if (i < num - 1) {
                code.append(",");
            }
        }
        return code.toString();
    }

    private void attributeCode(Set<RelationSchema.Attribute> attributes, StringBuilder columnNamesCode, StringBuilder tupleCode) {
        Iterator<RelationSchema.Attribute> iterator = attributes.iterator();
        while (iterator.hasNext()) {
            RelationSchema.Attribute attribute = iterator.next();
            columnNamesCode.append("\"").append(attribute.getName().toUpperCase()).append("\"");
            Class<?> type = attribute.getType();
            String conversionMethod = stringConversionMethods.get(type);
            int position = attribute.getPosition();
            if (!type.equals(Date.class)) {
                tupleCode.append("cells(").append(position).append(").").append(conversionMethod);
            } else {
                tupleCode.append(conversionMethod).append("(cells(").append(position).append("))");
            }

            if (iterator.hasNext()) {
                columnNamesCode.append(",");
                tupleCode.append(",");
            }
        }
    }

    private Set<RelationSchema.Attribute> extractAttributes() {
        Set<RelationSchema.Attribute> columnNames = new HashSet<>();

        relationProcessFunction.getSelectConditions().forEach(condition -> {
            attributeFromExpression(columnNames, condition.getExpression());
        });

        aggregateProcessFunction.getAggregateValues().forEach(aggregateValue -> {
            Value value = aggregateValue.getValue();
            if (value instanceof Expression) {
                attributeFromExpression(columnNames, (Expression) value);
                return;
            }
            attributeFromValue(columnNames, value);
        });

        return columnNames;
    }

    private void attributeFromExpression(Set<RelationSchema.Attribute> columnNames, Expression expression) {
        expression.getValues().forEach(value -> {
            attributeFromValue(columnNames, value);
        });
    }

    private void attributeFromValue(Set<RelationSchema.Attribute> columnNames, Value value) {
        if (value instanceof AttributeValue) {
            String lowerName = ((AttributeValue) value).getName().toLowerCase();
            RelationSchema.Attribute attribute = RelationSchema.getColumnAttribute(lowerName);
            if (attribute == null) {
                throw new RuntimeException("Unable to find attribute/column name in schema for: " + lowerName);
            }
            columnNames.add(attribute);
        }
    }
}