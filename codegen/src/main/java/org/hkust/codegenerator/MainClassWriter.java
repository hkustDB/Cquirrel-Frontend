package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.ainslec.picocog.PicoWriter;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.objects.*;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

class MainClassWriter implements ClassWriter {
    private static final String CLASS_NAME = "Job";
    private final List<AggregateProcessFunction> aggregateProcessFunctions;
    private final List<RelationProcessFunction> relationProcessFunctions;
    private final Map<Relation, Relation> joinStructure;
    private final String flinkInputPath;
    private final String flinkOutputPath;
    private final RelationSchema schema;
    private final Map<Relation, String> tagNames;
    private final Map<String, String> ACTIONS = ImmutableMap.of("Insert", "+", "Delete", "-");
    private static final Map<Class<?>, String> stringConversionMethods = new HashMap<>();

    static {
        stringConversionMethods.put(Integer.class, "toInt");
        stringConversionMethods.put(Double.class, "toDouble");
        stringConversionMethods.put(Long.class, "toLong");
        stringConversionMethods.put(Date.class, "format.parse");
    }

    MainClassWriter(Node node, RelationSchema schema, String flinkInputPath, String flinkOutputPath) {
        CheckerUtils.checkNullOrEmpty(flinkInputPath, "flinkInputPath");
        CheckerUtils.checkNullOrEmpty(flinkOutputPath, "flinkOutputPath");
        this.flinkInputPath = flinkInputPath;
        this.flinkOutputPath = flinkOutputPath;
        this.aggregateProcessFunctions = node.getAggregateProcessFunctions();
        this.relationProcessFunctions = node.getRelationProcessFunctions();
        this.joinStructure = node.getJoinStructure();
        this.schema = schema;
        this.tagNames = new HashMap<>();
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            tagNames.put(rpf.getRelation(), rpf.getRelation().getValue().toLowerCase() + "Tag");
        }
    }

    @Override
    public String write(String filePath) throws Exception {
        final PicoWriter writer = new PicoWriter();

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
        relationProcessFunctions.forEach(rpf -> {
            writer.writeln(tagNames.get(rpf.getRelation()) + ": OutputTag[Payload] = OutputTag[Payload](\"" + rpf.getRelation().getValue() + "\")");
        });
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
        tagNames.forEach((key, value) -> writer.writeln("val " + key.getValue().toLowerCase() + " : DataStream[Payload] = inputStream.getSideOutput(" + value + ")"));
        if (relationProcessFunctions.size() == 1) {
            writeRootStream(relationProcessFunctions.get(0), writer);
        } else {
            RelationProcessFunction leaf = getLeafOrParent(true);
            RelationProcessFunction parent = getRelation(joinStructure.get(leaf.getRelation()));
            while (parent != null) {
                writeStream(leaf, parent, writer);
                leaf = parent;
                parent = getRelation(joinStructure.get(parent.getRelation()));
            }
            RelationProcessFunction root = getLeafOrParent(false);

            writeRootStream(root, writer);
        }
    }

    @NotNull
    private RelationProcessFunction getLeafOrParent(boolean leaf) {
        RelationProcessFunction relationProcessFunction = null;
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            if ((leaf ? rpf.isLeaf() : rpf.isRoot())) {
                relationProcessFunction = rpf;
            }
        }
        if (relationProcessFunction == null) {
            throw new RuntimeException("No relation process function found in " + relationProcessFunctions);
        }
        return relationProcessFunction;
    }

    private void writeRootStream(RelationProcessFunction root, final PicoWriter writer) {
        writer.writeln("val result  = inputStream.keyBy(i => i._3)");
        String className = getProcessFunctionClassName(root.getName());
        writer.writeln(".process(new " + className + "())");
        writer.writeln(".keyBy(i => i._3)");
        writer.writeln(".process(new " + className + "())");
        writer.writeln(".map(x => (x._4.mkString(\", \"), x._5.mkString(\", \"), x._6))");
        writer.writeln(".writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE)");
        writer.writeln(".setParallelism(1)");
        writer.writeln("env.execute(\"Flink Streaming Scala API Skeleton\")");
        writer.writeln_l("}");
    }

    private void writeStream(RelationProcessFunction relationProcessFunction, RelationProcessFunction parent, final PicoWriter writer) {
        String streamSuffix = "S";
        String leafName = relationProcessFunction.getName();
        writer.writeln("val " + leafName + streamSuffix + " = " + leafName + ".keyBy(i => i._3)");
        writer.writeln(".process(new " + getProcessFunctionClassName(leafName) + "())");
        Relation leafParent = joinStructure.get(relationProcessFunction.getRelation());
        if (leafParent == null) {
            throw new RuntimeException("No relationProcessFunction parent found for " + relationProcessFunction);

        }
        writer.writeln(".connect(" + leafParent.getValue() + ")");
        writer.writeln(".process(new " + getProcessFunctionClassName(parent.getName()) + ")");
    }

    @Nullable
    private RelationProcessFunction getRelation(Relation relation) {
        for (RelationProcessFunction rpf : relationProcessFunctions) {
            if (rpf.getRelation() == relation) {
                return rpf;
            }
        }
        return null;
    }

    @VisibleForTesting
    void addGetStreamFunction(final PicoWriter writer) throws Exception {
        writer.writeln_r("private def getStream(env: StreamExecutionEnvironment, dataPath: String): DataStream[Payload] = {");
        writer.writeln("val data = env.readTextFile(dataPath).setParallelism(1)");
        writer.writeln("val format = new java.text.SimpleDateFormat(\"yyyy-MM-dd\")");
        writer.writeln("var cnt : Long = 0");
        writer.writeln("val restDS : DataStream[Payload] = data");
        writer.writeln(".process((value: String, ctx: ProcessFunction[String, Payload]#Context, out: Collector[Payload]) => {");
        writer.writeln("val header = line.substring(0,3)");
        writer.writeln("val cells : Array[String] = line.substring(3).split(\"\\\\|\")");
        writer.writeln("var relation = \"\"");
        writer.writeln("var action = \"\"");
        writer.writeln_r("header match {");

        for (RelationProcessFunction rpf : relationProcessFunctions) {
            Relation relation = rpf.getRelation();
            Set<Attribute> attributes = extractAttributes(rpf);
            String lowerRelationName = relation.getValue();
            StringBuilder columnNamesCode = new StringBuilder();
            StringBuilder tupleCode = new StringBuilder();
            attributeCode(attributes, columnNamesCode, tupleCode);
            String caseLabel = caseLabel(relation);
            ACTIONS.forEach((key, value) -> {
                writer.writeln("case \"" + value + caseLabel + "\" =>");
                writer.writeln("action = \"" + key + "\"");
                writer.writeln("relation = \"" + lowerRelationName + "\"");
                writer.writeln("val i = Tuple" + attributes.size() + "(" + tupleCode.toString() + ")");
                writer.writeln("cnt = cnt + 1");
                writer.writeln("ctx.output(" + tagNames.get(rpf.getRelation()) + ", Payload(relation, action, cells(0).toInt.asInstanceOf[Any],");
                writer.writeln("Array[Any](" + iteratorCode(attributes.size()) + "),");
                writer.writeln("Array(" + columnNamesCode.toString() + "), cnt)");
            });
            writer.writeln("case _ =>");
            writer.writeln("out.collect(Payload(\"\", \"\", 0, Array(), Array(), 0))");
            writer.writeln("}");
        }
        writer.writeln("}).setParallelism(1)");
        writer.writeln("restDS");
        writer.writeln_l("}");
    }

    private String caseLabel(Relation relation) {
        return relation.getValue().substring(0, 2).toUpperCase();
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
    void attributeCode(Set<Attribute> attributes, StringBuilder columnNamesCode, StringBuilder tupleCode) {
        Iterator<Attribute> iterator = attributes.iterator();
        while (iterator.hasNext()) {
            Attribute attribute = iterator.next();
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

    private Set<Attribute> extractAttributes(RelationProcessFunction rpf) throws Exception {
        Set<Attribute> columnNames = new LinkedHashSet<>();
        Relation relation = rpf.getRelation();
        for (SelectCondition condition : rpf.getSelectConditions()) {
            attributeFromExpression(relation, columnNames, condition.getExpression());
        }

        //TODO: currently only one aggregate process function is supported
        for (AggregateProcessFunction.AggregateValue aggregateValue : aggregateProcessFunctions.get(0).getAggregateValues()) {
            Value value = aggregateValue.getValue();
            if (value instanceof Expression) {
                attributeFromExpression(relation, columnNames, (Expression) value);
                continue;
            }
            attributeFromValue(relation, columnNames, value);
        }

        return columnNames;
    }

    private void attributeFromExpression(Relation relation, Set<Attribute> columnNames, Expression expression) throws Exception {
        for (Value value : expression.getValues()) {
            attributeFromValue(relation, columnNames, value);
        }
    }

    private void attributeFromValue(Relation relation, Set<Attribute> columnNames, Value value) throws Exception {
        //Only AttributeValue entertained. Perhaps visitor pattern to avoid multiple if/else blocks?
        if (value instanceof AttributeValue) {
            String lowerName = ((AttributeValue) value).getColumnName().toLowerCase();
            Attribute attribute = schema.getColumnAttribute(relation, lowerName);
            if (attribute == null) {
                return;
                //TODO: do we need to throw? Currently I am adding the attributes from the AggregateValue that are also present in the relation in question's schema
                //throw new RuntimeException("Unable to find attribute/column name in schema for: " + relation + "." + lowerName);
            }
            columnNames.add(attribute);
        }
    }
}