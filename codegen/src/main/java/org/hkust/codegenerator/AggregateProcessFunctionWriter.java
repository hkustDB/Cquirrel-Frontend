package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.*;
import org.hkust.schema.RelationSchema;

import java.util.List;

class AggregateProcessFunctionWriter extends ProcessFunctionWriter {
    private final PicoWriter writer = new PicoWriter();
    private final AggregateProcessFunction aggregateProcessFunction;
    private final String aggregateType;
    private final String className;
    private final RelationSchema relationSchema;

    AggregateProcessFunctionWriter(final AggregateProcessFunction aggregateProcessFunction, RelationSchema schema) {
        super(schema);
        this.relationSchema = schema;
        this.aggregateProcessFunction = aggregateProcessFunction;
        Class<?> type = aggregateProcessFunction.getValueType();
        aggregateType = type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName();
        className = getProcessFunctionClassName(aggregateProcessFunction.getName());
    }

    @Override
    public String write(String filePath) throws Exception {
        addImports(writer);
        addConstructorAndOpenClass(writer);
        addAggregateFunction(writer);
        addAdditionFunction(writer);
        addSubtractionFunction(writer);
        List<SelectCondition> aggregateSelectConditions = aggregateProcessFunction.getAggregateSelectCondition();
        if (aggregateSelectConditions != null && !aggregateSelectConditions.isEmpty()) {
            addIsOutputValidFunction(writer, aggregateSelectConditions);
        }
        addInitStateFunction(writer);
        closeClass(writer);
        writeClassFile(className, filePath, writer.toString());

        return className;
    }

    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import org.apache.flink.api.common.state.ValueStateDescriptor");
        writer.writeln("import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}");
        writer.writeln("import org.hkust.BasedProcessFunctions.AggregateProcessFunction");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        //TODO: apply the next_key optimization on thiskey, remember: next_key is now output_key and requires no such optimizations
        List<AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        String code = "class " +
                className +
                " extends AggregateProcessFunction[Any, " +
                aggregateType +
                "](\"" +
                className +
                "\", " +
                keyListToCode(optimizeKey(aggregateProcessFunction.getThisKey())) +
                ", " +
                keyListToCode(aggregateProcessFunction.getOutputKey()) +
                "," +
                " aggregateName = \"" +
                (aggregateValues.size() == 1 ? aggregateValues.get(0).getName() : "_multiple_") + "\"" +
                ", deltaOutput = true" +
                ") {";
        writer.writeln_r(code);
    }

    @VisibleForTesting
    void addAggregateFunction(final PicoWriter writer) throws Exception {
        writer.writeln_r("override def aggregate(value: Payload): " + aggregateType + " = {");
        List<AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        for (AggregateValue aggregateValue : aggregateValues) {
            StringBuilder code = new StringBuilder();
            String type = aggregateValue.getType();
            if (type.equals("expression")) {
                Expression expression = (Expression) aggregateValue.getValue();
                expressionToCode(expression, code);
                writer.writeln(code.toString());
            } else if (type.equals("attribute")) {
                AttributeValue attributeValue = (AttributeValue) aggregateValue.getValue();
                attributeValueToCode(attributeValue, code);
                writer.writeln(code.toString());
            } else if (type.equals("constant")) {
                constantValueToCode((ConstantValue) aggregateValue.getValue(), code);
                writer.writeln(code.toString());
            } else {
                throw new RuntimeException("Unknown type for AggregateValue, got: " + type);
            }
        }
        writer.writeln_l("}");
    }

    void addIsOutputValidFunction(final PicoWriter writer, List<SelectCondition> aggregateSelectConditions) throws Exception {
        if (aggregateSelectConditions.size() != 1) {
            throw new RuntimeException("Currently only 1 aggregate select condition is supported");
        }
        SelectCondition condition = aggregateSelectConditions.get(0);
        StringBuilder code = new StringBuilder();
        code.append("override def isOutputValid(value: Payload): Boolean = {");
        code.append("if(");
        expressionToCode(condition.getExpression(), code);
        writer.write(code.toString());
        writer.writeln_r("){");
        writer.write("true");
        writer.writeln_lr("}else{");
        writer.write("false");
        writer.writeln_l("}");
        writer.writeln_l("}");
    }

    @VisibleForTesting
    void addAdditionFunction(final PicoWriter writer) {
        writer.writeln("override def addition(value1: " + aggregateType + ", value2: " + aggregateType + "): " + aggregateType + " = value1 + value2");
    }

    @VisibleForTesting
    void addSubtractionFunction(final PicoWriter writer) {
        writer.writeln("override def subtraction(value1: " + aggregateType + ", value2: " + aggregateType + "): " + aggregateType + " = value1 - value2");
    }

    @VisibleForTesting
    void addInitStateFunction(final PicoWriter writer) {
        writer.writeln_r("override def initstate(): Unit = {");
        writer.writeln("val valueDescriptor = TypeInformation.of(new TypeHint[" + aggregateType + "](){})");
        writer.writeln("val aliveDescriptor : ValueStateDescriptor[" + aggregateType + "] = new ValueStateDescriptor[" + aggregateType + "](\"" + className + "\"+\"Alive\", valueDescriptor)");
        writer.writeln("alive = getRuntimeContext.getState(aliveDescriptor)");
        writer.writeln_r("}");
        //TODO: later substitute with default value for the class in question. May need to have a map for this as some don't have a default constructor e.g. Double
        writer.writeln("override val init_value: " + aggregateType + " = 0.0");
    }
}
