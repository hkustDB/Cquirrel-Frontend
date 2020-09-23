import org.ainslec.picocog.PicoWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunctionWriter implements ProcessFunctionWriter {
    private static final String CLASS_NAME = "AggregateProcessFunction";
    private static final PicoWriter writer = new PicoWriter();
    private final AggregateProcessFunction aggregateProcessFunction;
    private final String aggregateType;
    private final String className;

    public AggregateProcessFunctionWriter(final AggregateProcessFunction aggregateProcessFunction) {
        requireNonNull(aggregateProcessFunction);
        this.aggregateProcessFunction = aggregateProcessFunction;
        Class type = aggregateProcessFunction.getValueType();
        aggregateType = type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName();
        className = aggregateProcessFunction.getName();
    }

    @Override
    public void generateCode(String filePath) throws IOException {
        addImports();
        addConstructorAndOpenClass();
        addAggregateFunction();
        addAdditionFunction();
        addSubtractionFunction();
        addInitStateFunction();
        //class closing
        writer.writeln_l("}");
        Files.write(Paths.get(filePath + File.separator + className + ".scala"), writer.toString().getBytes());
    }

    private void addImports() {
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import org.apache.flink.api.common.state.ValueStateDescriptor");
        writer.writeln("import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}");
        writer.writeln("import org.hkust.BasedProcessFunctions.AggregateProcessFunction");
    }

    private void addConstructorAndOpenClass() {
        List<AggregateProcessFunction.AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        writer.writeln_r("class " + className + " extends AggregateProcessFunction[Any, " + aggregateType + "](\"" + className + "\", Array(), Array(),aggregateName = \""
                + (aggregateValues.size() == 1 ? aggregateValues.get(0).getName() : "_multiple_")
                + "\") {");
    }

    private void addAggregateFunction() {
        writer.writeln_r("override def aggregate(value: Payload): " + aggregateType + " = {");
        List<AggregateProcessFunction.AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        aggregateValues.forEach(aggregateValue -> {
            StringBuilder code;
            if (aggregateValue.getType().equals("expression")) {
                Expression expression = (Expression) aggregateValue.getValue();
                List<Value> values = expression.getValues();
                for (int i = 0; i < values.size(); i++) {
                    code = new StringBuilder();
                    Value value = values.get(i);
                    if (!(value instanceof AttributeValue)) {
                        throw new RuntimeException("Currently only attribute values supported for expression values");
                    }
                    AttributeValue attributeValue = (AttributeValue) value;
                    String columnName = attributeValue.getName();
                    Class type = RelationSchema.getColumnAttribute(columnName.toLowerCase()).getType();
                    code.append("value(\"")
                            .append(columnName.toUpperCase())
                            .append("\")")
                            .append(".asInstanceOf[")
                            .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                            .append("]");
                    writer.write(code.toString());
                    if (i == values.size() - 1) {
                        break;
                    }
                    writer.write(expression.getOperator().getValue());
                }
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
        writer.writeln("val aliveDescriptor : ValueStateDescriptor[" + aggregateType + "] = new ValueStateDescriptor[" + aggregateType + "](name+\"Alive\", valueDescriptor)");
        writer.writeln("alive = getRuntimeContext.getState(aliveDescriptor)");
        writer.writeln_r("}");
        //TODO: later substitute with default value for the class in question. May need to have a map for this as some don't have a default constructor e.g. Double
        writer.writeln("override val init_value: " + aggregateType + " = 0.0");
    }
}
