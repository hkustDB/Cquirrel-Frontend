import org.ainslec.picocog.PicoWriter;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunctionWriter extends ProcessFunctionWriter {
    private static final PicoWriter writer = new PicoWriter();
    private final AggregateProcessFunction aggregateProcessFunction;
    private final String aggregateType;
    private final String className;

    public AggregateProcessFunctionWriter(final AggregateProcessFunction aggregateProcessFunction) {
        requireNonNull(aggregateProcessFunction);
        this.aggregateProcessFunction = aggregateProcessFunction;
        Class type = aggregateProcessFunction.getValueType();
        aggregateType = type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName();
        className = makeClassName(aggregateProcessFunction.getName());
    }

    @Override
    public String generateCode(String filePath) throws IOException {
        addImports();
        addConstructorAndOpenClass();
        addAggregateFunction();
        addAdditionFunction();
        addSubtractionFunction();
        addInitStateFunction();
        closeClass(writer);
        writeClassFile(className, filePath, writer.toString());

        return className;
    }

    @Override
    public void addImports() {
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import org.apache.flink.api.common.state.ValueStateDescriptor");
        writer.writeln("import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}");
        writer.writeln("import org.hkust.BasedProcessFunctions.AggregateProcessFunction");
    }

    @Override
    public void addConstructorAndOpenClass() {
        List<AggregateProcessFunction.AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        String code = "class " +
                className +
                " extends AggregateProcessFunction[Any, " +
                aggregateType +
                "](\"" +
                className +
                "\", " +
                keyListToCode(aggregateProcessFunction.getThisKey()) +
                ", " +
                keyListToCode(aggregateProcessFunction.getNextKey()) +
                "," +
                " aggregateName = \"" +
                (aggregateValues.size() == 1 ? aggregateValues.get(0).getName() : "_multiple_") +
                "\") {";
        writer.writeln_r(code);
    }

    private void addAggregateFunction() {
        writer.writeln_r("override def aggregate(value: Payload): " + aggregateType + " = {");
        List<AggregateProcessFunction.AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        aggregateValues.forEach(aggregateValue -> {
            StringBuilder code = new StringBuilder();
            if (aggregateValue.getType().equals("expression")) {
                Expression expression = (Expression) aggregateValue.getValue();
                expressionToCode(expression, code);
                writer.writeln(code.toString());
            } else {
                throw new RuntimeException("Only Expression type is supported for AggregateValue");
            }
        });
        writer.writeln_l("}");
    }

    private void addAdditionFunction() {
        writer.writeln("override def addition(value1: " + aggregateType + ", value2: " + aggregateType + "): " + aggregateType + " = value1 + value2");
    }

    private void addSubtractionFunction() {
        writer.writeln("override def subtraction(value1: " + aggregateType + ", value2: " + aggregateType + "): " + aggregateType + " = value1 - value2");
    }

    private void addInitStateFunction() {
        writer.writeln_r("override def initstate(): Unit = {");
        writer.writeln("val valueDescriptor = TypeInformation.of(new TypeHint[" + aggregateType + "](){})");
        writer.writeln("val aliveDescriptor : ValueStateDescriptor[" + aggregateType + "] = new ValueStateDescriptor[" + aggregateType + "](\"" + className + "\"+\"Alive\", valueDescriptor)");
        writer.writeln("alive = getRuntimeContext.getState(aliveDescriptor)");
        writer.writeln_r("}");
        //TODO: later substitute with default value for the class in question. May need to have a map for this as some don't have a default constructor e.g. Double
        writer.writeln("override val init_value: " + aggregateType + " = 0.0");
    }
}
