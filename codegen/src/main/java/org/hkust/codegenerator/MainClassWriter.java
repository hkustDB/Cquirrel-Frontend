package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.ainslec.picocog.PicoWriter;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.objects.AggregateProcessFunction;
import org.hkust.objects.Node;
import org.hkust.objects.RelationProcessFunction;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static java.util.Objects.requireNonNull;
import static org.hkust.objects.Type.getStringConversionMethod;

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
    private Boolean isFileSink = false;
    private Boolean isSocketSink = false;
    private Boolean isKafkaSink = false;


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

    MainClassWriter(Node node, RelationSchema schema, String flinkInputPath, String flinkOutputPath, String[] dataSinkTypes) {
        this(node, schema, flinkInputPath, flinkOutputPath);
        for (String sinkType : dataSinkTypes) {
            if (sinkType.equals("socket")) {
                this.isSocketSink = true;
            }
            if (sinkType.equals("kafka")) {
                this.isKafkaSink = true;
            }
            if (sinkType.equals("file")) {
                this.isFileSink = true;
            }
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
        writer.writeln("import org.apache.flink.streaming.api.functions.ProcessFunction");
        writer.writeln("import org.apache.flink.util.Collector");
        writer.writeln("import org.apache.flink.api.common.serialization.SimpleStringSchema");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        writer.writeln_r("object " + CLASS_NAME + " {");
        relationProcessFunctions.forEach(rpf -> {
            writer.writeln("val " + tagNames.get(rpf.getRelation()) + ": OutputTag[Payload] = OutputTag[Payload](\"" + rpf.getRelation().getValue() + "\")");
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
        tagNames.forEach((key, value) -> writer.writeln("val " + key.toString().toLowerCase() + " : DataStream[Payload] = inputStream.getSideOutput(" + value + ")"));
        if (relationProcessFunctions.size() == 1) {
            writeSingleRelationStream(relationProcessFunctions.get(0), writer);
        } else {
            RelationProcessFunction root = getLeafOrParent(true);
            writeMultipleRelationStream(root, writer, "", "S");
        }
        writer.writeln("env.execute(\"Flink Streaming Scala API Skeleton\")");
        writer.writeln_l("}");
    }

    private void writeDataSink(final PicoWriter writer) {
        if (this.isSocketSink) {
            writer.writeln("result.map(x => x.toString()).writeToSocket(\"localhost\",5001,new SimpleStringSchema()).setParallelism(1)");
        }
        if (this.isFileSink) {
            writer.writeln("result.writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE).setParallelism(1)");
        }
    }

    private void writeMultipleRelationStream(RelationProcessFunction rpf, final PicoWriter writer, String prevStreamName, String streamSuffix) {
        Relation relation = rpf.getRelation();
        String relationName = relation.toString().toLowerCase();
        String streamName = relationName + streamSuffix;
        if (rpf.isLeaf()) {
            writer.writeln("val " + streamName + " = " + relationName + ".keyBy(i => i._3)");
        } else {
            writer.writeln("val " + streamName + " = " + prevStreamName + ".connect(" + relationName + ")");
            writer.writeln(".keyBy(i => i._3, i => i._3)");
        }
        writer.writeln(".process(new " + getProcessFunctionClassName(rpf.getName()) + "())");
        RelationProcessFunction parent = getRelation(joinStructure.get(relation));
        if (parent == null) {
            writer.writeln("val result = " + streamName + ".keyBy(i => i._3)");
            linkAggregateProcessFunctions(writer);
            writeDataSink(writer);
            return;
        }
        writeMultipleRelationStream(requireNonNull(parent),
                writer,
                streamName,
                streamSuffix
        );
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

    private void writeSingleRelationStream(RelationProcessFunction root, final PicoWriter writer) {
        writer.writeln("val result = " + root.getRelation().toString().toLowerCase() + ".keyBy(i => i._3)");
        String className = getProcessFunctionClassName(root.getName());
        writer.writeln(".process(new " + className + "())");
        writer.writeln(".keyBy(i => i._3)");
        linkAggregateProcessFunctions(writer);
        writeDataSink(writer);
    }

    private void linkAggregateProcessFunctions(final PicoWriter writer) {
        int size = aggregateProcessFunctions.size();
        if (size == 1) {
            writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunctions.get(0).getName()) + "())");
            writer.writeln(".map(x => x._4.mkString(\"|\") + \"|\" + x._5.mkString(\"|\")+ \"|\" + x._6)");
        } else if (size == 2) {
            writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunctions.get(0).getName()) + "())");
            writer.writeln(".map(x => Payload(\"Aggregate\", \"Addition\", x._3, x._4, x._5, x._6))");
            writer.writeln(".keyBy(i => i._3)");
            writer.writeln(".process(new " + getProcessFunctionClassName(aggregateProcessFunctions.get(1).getName()) + "())");
            writer.writeln(".map(x => x._4.mkString(\"|\") + \"|\" + x._5.mkString(\"|\")+ \"|\" + x._6)");
        } else {
            throw new RuntimeException("Currently only 1 or 2 aggregate process functions are supported");
        }
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
    void addGetStreamFunction(final PicoWriter writer) {
        writer.writeln_r("private def getStream(env: StreamExecutionEnvironment, dataPath: String): DataStream[Payload] = {");
        writer.writeln("val data = env.readTextFile(dataPath).setParallelism(1)");
        writer.writeln("val format = new java.text.SimpleDateFormat(\"yyyy-MM-dd\")");
        writer.writeln("var cnt : Long = 0");
        writer.writeln("val restDS : DataStream[Payload] = data");
        writer.writeln(".process((value: String, ctx: ProcessFunction[String, Payload]#Context, out: Collector[Payload]) => {");
        writer.writeln("val header = value.substring(0,3)");
        writer.writeln("val cells : Array[String] = value.substring(3).split(\"\\\\|\")");
        writer.writeln("var relation = \"\"");
        writer.writeln("var action = \"\"");
        writer.writeln_r("header match {");

        Set<Attribute> attributes = new HashSet<>();
        for (RelationProcessFunction relationProcessFunction : relationProcessFunctions) {
            attributes.addAll(relationProcessFunction.getAttributeSet(schema));
        }
        attributes.addAll(aggregateProcessFunctions.get(0).getAttributeSet(schema));

        for (RelationProcessFunction rpf : relationProcessFunctions) {
            Relation relation = rpf.getRelation();
            String lowerRelationName = relation.getValue();
            StringBuilder columnNamesCode = new StringBuilder();
            StringBuilder tupleCode = new StringBuilder();
            int numberOfMatchingColumns = attributeCode(rpf, attributes, columnNamesCode, tupleCode);
            String caseLabel = caseLabel(relation);
            ACTIONS.forEach((key, value) -> {
                writer.writeln("case \"" + value + caseLabel + "\" =>");
                writer.writeln("action = \"" + key + "\"");
                writer.writeln("relation = \"" + lowerRelationName + "\"");
                writer.writeln("val i = Tuple" + numberOfMatchingColumns + "(" + tupleCode.toString() + ")");
                writer.writeln("cnt = cnt + 1");
                writer.writeln("ctx.output(" + tagNames.get(rpf.getRelation()) + ", Payload(relation, action, " + thisKeyCode(rpf));
                writer.writeln("Array[Any](" + iteratorCode(numberOfMatchingColumns) + "),");
                writer.writeln("Array[String](" + columnNamesCode.toString() + "), cnt))");
            });
        }
        writer.writeln("case _ =>");
        writer.writeln("out.collect(Payload(\"\", \"\", 0, Array(), Array(), 0))");
        writer.writeln("}");
        writer.writeln("}).setParallelism(1)");
        writer.writeln("restDS");
        writer.writeln_l("}");
    }

    private String caseLabel(Relation relation) {
        return relation.getValue().substring(0, 2).toUpperCase();
    }

    private String thisKeyCode(RelationProcessFunction rpf) {
        List<String> thisKeyAttributes = rpf.getThisKey();
        int rpfThisKeySize = thisKeyAttributes.size();
        if (rpfThisKeySize == 1) {
            String thisKey = thisKeyAttributes.get(0);
            Attribute keyAttribute = schema.getColumnAttributeByRawName(rpf.getRelation(), thisKey);
            requireNonNull(keyAttribute);
            return "cells(" + keyAttribute.getPosition() + ")." + getStringConversionMethod(keyAttribute.getType()) + ".asInstanceOf[Any],";
        } else if (rpfThisKeySize == 2) {
            String thisKey1 = thisKeyAttributes.get(0);
            String thisKey2 = thisKeyAttributes.get(1);
            Attribute keyAttribute1 = schema.getColumnAttributeByRawName(rpf.getRelation(), thisKey1);
            requireNonNull(keyAttribute1);
            Attribute keyAttribute2 = schema.getColumnAttributeByRawName(rpf.getRelation(), thisKey2);
            requireNonNull(keyAttribute2);
            return "Tuple2( cells(" + keyAttribute1.getPosition() + ")." + getStringConversionMethod(keyAttribute1.getType()) +
                    ", cells(" + keyAttribute2.getPosition() + ")." + getStringConversionMethod(keyAttribute2.getType()) + ").asInstanceOf[Any],";
        } else {
            throw new RuntimeException("Expecting 1 or 2 thisKey values got: " + rpfThisKeySize);
        }
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
    int attributeCode(RelationProcessFunction rpf, Set<Attribute> agfAttributes, StringBuilder columnNamesCode, StringBuilder tupleCode) {
        Set<Attribute> attributes = new LinkedHashSet<>(agfAttributes);

        List<String> agfNextKeys = aggregateProcessFunctions.get(0).getOutputKey();
        if (agfNextKeys != null) {
            for (String key : agfNextKeys) {
                Attribute attribute = schema.getColumnAttributeByRawName(rpf.getRelation(), key);
                if (attribute != null) {
                    attributes.add(attribute);
                }
            }
        }

        List<String> agfThisKeys = aggregateProcessFunctions.get(0).getThisKey();
        if (agfThisKeys != null) {
            for (String key : agfThisKeys) {
                Attribute attribute = schema.getColumnAttributeByRawName(rpf.getRelation(), key);
                if (attribute != null) {
                    attributes.add(attribute);
                }
            }
        }

        Iterator<Attribute> iterator = attributes.iterator();
        int numberOfMatchingColumns = 0;
        while (iterator.hasNext()) {
            Attribute attribute = iterator.next();
            Attribute rpfAttribute = schema.getColumnAttributeByRawName(rpf.getRelation(), attribute.getName());
            if (rpfAttribute == null || !rpfAttribute.equals(attribute)) {
                if (!iterator.hasNext()) {
                    columnNamesCode.delete(columnNamesCode.length() - ",".length(), columnNamesCode.length());
                    tupleCode.delete(tupleCode.length() - ",".length(), tupleCode.length());
                }
                continue;
            }
            numberOfMatchingColumns++;


            columnNamesCode.append("\"").append(attribute.getName().toUpperCase()).append("\"");
            Class<?> type = attribute.getType();
            String conversionMethod = getStringConversionMethod(type);
            int position = attribute.getPosition();
            if (!type.equals(Date.class)) {
                tupleCode.append("cells(").append(position).append(")").append(conversionMethod == null ? "" : "." + conversionMethod);
            } else {
                tupleCode.append(conversionMethod).append("(cells(").append(position).append("))");
            }

            if (iterator.hasNext()) {
                columnNamesCode.append(",");
                tupleCode.append(",");
            }
        }

        return numberOfMatchingColumns;
    }
}
