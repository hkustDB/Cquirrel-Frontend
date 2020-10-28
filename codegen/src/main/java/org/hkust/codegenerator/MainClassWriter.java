package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import org.ainslec.picocog.PicoWriter;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.objects.*;

import java.io.IOException;
import java.util.*;

class MainClassWriter implements ClassWriter {
    private static final String CLASS_NAME = "Job";
    private final AggregateProcessFunction aggregateProcessFunction;
    private final String aggregateProcFuncClassName;
    private final RelationProcessFunction relationProcessFunction;
    private final String relationProcFuncClassName;
    private final String flinkInputPath;
    private final String flinkOutputPath;
    private final PicoWriter writer = new PicoWriter();

    private static final Map<Class, String> stringConversionMethods = new HashMap<>();

    static {
        stringConversionMethods.put(Integer.class, "toInt");
        stringConversionMethods.put(Double.class, "toDouble");
        stringConversionMethods.put(Long.class, "toLong");
        stringConversionMethods.put(Date.class, "format.parse");
    }

    MainClassWriter(Node node, String flinkInputPath, String flinkOutputPath) {
        CheckerUtils.checkNullOrEmpty(flinkInputPath, "flinkInputPath");
        CheckerUtils.checkNullOrEmpty(flinkOutputPath, "flinkOutputPath");
        this.flinkInputPath = flinkInputPath;
        this.flinkOutputPath = flinkOutputPath;
        this.aggregateProcessFunction = node.getAggregateProcessFunction();
        this.aggregateProcFuncClassName = getProcessFunctionClassName(aggregateProcessFunction.getName());
        this.relationProcessFunction = node.getRelationProcessFunction();
        this.relationProcFuncClassName = getProcessFunctionClassName(relationProcessFunction.getName());
    }

    @Override
    public String write(String filePath) throws IOException {
        addImports(writer);
        addConstructorAndOpenClass(writer);
        addMainFunction(writer);
        addGetStreamFunction(writer);
        closeClass(writer);
        writeClassFile(CLASS_NAME, filePath, writer.toString());

        return CLASS_NAME;
    }

    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import org.apache.flink.api.java.utils.ParameterTool");
        writer.writeln("import org.apache.flink.core.fs.FileSystem");
        writer.writeln("import org.apache.flink.streaming.api.TimeCharacteristic");
        writer.writeln("import org.apache.flink.streaming.api.scala._");
        writer.writeln("import org.hkust.RelationType.Payload");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        writer.writeln_r("object " + CLASS_NAME + " {");
    }

    @VisibleForTesting
    void addMainFunction(final PicoWriter writer) {
        writer.writeln_r("def main(args: Array[String]) {");
        writer.writeln("val env = StreamExecutionEnvironment.getExecutionEnvironment");
        writer.writeln("val params: ParameterTool = ParameterTool.fromArgs(args)");
        writer.writeln("env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)");
        writer.writeln("var executionConfig = env.getConfig");
        writer.writeln("executionConfig.enableObjectReuse()");
        writer.writeln("val inputpath = \"" + flinkInputPath + "\"");
        writer.writeln("val outputpath = \"" + flinkOutputPath + "\"");
        writer.writeln("val inputStream : DataStream[Payload] = getStream(env,inputpath)");
        writer.writeln("val result  = inputStream.keyBy(i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(relationProcessFunction.getName()) + "())");
        writer.writeln(".keyBy(i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunction.getName()) + ")");
        writer.writeln(".map(x => (x._4.mkString(\", \"), x._5.mkString(\", \"), x._6))");
        writer.writeln(".writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE)");
        writer.writeln(".setParallelism(1)");
        writer.writeln("env.execute(\"Flink Streaming Scala API Skeleton\")");
        writer.writeln_l("}");
    }

    @VisibleForTesting
    void addGetStreamFunction(final PicoWriter writer) {
        Set<RelationSchema.Attribute> attributes = extractAttributes();
        StringBuilder columnNamesCode = new StringBuilder();
        StringBuilder tupleCode = new StringBuilder();
        attributeCode(attributes, columnNamesCode, tupleCode);
        String lowerRelationName = relationProcessFunction.getRelationName().toLowerCase();
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
        writer.writeln("relation = \"" + lowerRelationName + "\"");
        writer.writeln("action = \"Insert\"");
        writer.writeln("case \"-LI\" =>");
        writer.writeln("relation = \"" + lowerRelationName + "\"");
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
    @VisibleForTesting
    void attributeCode(Set<RelationSchema.Attribute> attributes, StringBuilder columnNamesCode, StringBuilder tupleCode) {
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
        Set<RelationSchema.Attribute> columnNames = new LinkedHashSet<>();

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
        //Only AttributeValue entertained. Perhaps visitor pattern to avoid multiple if/else blocks?
        if (value instanceof AttributeValue) {
            String lowerName = ((AttributeValue) value).getColumnName().toLowerCase();
            RelationSchema.Attribute attribute = RelationSchema.getColumnAttribute(lowerName);
            if (attribute == null) {
                throw new RuntimeException("Unable to find attribute/column name in schema for: " + lowerName);
            }
            columnNames.add(attribute);
        }
    }
}