import org.ainslec.picocog.PicoWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunctionWriter implements ProcessFunctionWriter {
    private static final String CLASS_NAME = "AggregateProcessFunction";
    private static final PicoWriter writer = new PicoWriter();
    private final AggregateProcessFunction aggregateProcessFunction;

    public AggregateProcessFunctionWriter(final AggregateProcessFunction aggregateProcessFunction) {
        requireNonNull(aggregateProcessFunction);
        this.aggregateProcessFunction = aggregateProcessFunction;
    }

    @Override
    public void generateCode(String filePath) throws IOException {
        addImports();
        addConstructorAndOpenClass();
    }

    private void addImports() {
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import org.apache.flink.api.common.state.ValueStateDescriptor");
        writer.writeln("import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}");
        writer.writeln("import org.hkust.BasedProcessFunctions.AggregateProcessFunction");
    }

    private void addConstructorAndOpenClass() {
        List<AggregateProcessFunction.AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        writer.writeln("class " + aggregateProcessFunction.getName() + " extends AggregateProcessFunction[Any, Double](\"" + aggregateProcessFunction.getName() + "\", Array(), Array(),aggregateName = \""
                + (aggregateValues.size() == 1 ? aggregateValues.get(0).getName() : "_multiple_")
                + "\") {");
    }

    private void addAggregateFunction() {
        writer.writeln_l("override def aggregate(value: Payload): Double = {");
        List<AggregateProcessFunction.AggregateValue> aggregateValues = aggregateProcessFunction.getAggregateValues();
        List<String> aggregationCodes = new ArrayList<>();
        StringBuilder code = new StringBuilder();
        aggregateValues.forEach(aggregateValue -> {
            if (aggregateValue.getType().equals("expression")) {
                Expression expression = (Expression) aggregateValue.getValue();
                expression.getValues().forEach(value -> {
                    if (!(value instanceof AttributeValue)) {
                        throw new RuntimeException("Currently only attribute values supported for expression values");
                    }
                    AttributeValue attributeValue = (AttributeValue) value;
                    code.append("value(\"").append(((AttributeValue) value).getName());
                });
            } else {
                throw new RuntimeException("Only Expression type is supported for AggregateValue");
            }
        });
        writer.writeln(code.toString());
    }
}
