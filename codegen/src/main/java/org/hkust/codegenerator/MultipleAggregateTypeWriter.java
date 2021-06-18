package org.hkust.codegenerator;

import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.AggregateProcessFunction;
import org.hkust.objects.AggregateValue;
import org.hkust.objects.Operator;

import java.io.IOException;


public class MultipleAggregateTypeWriter implements ClassWriter {
    private final String className;
    private final PicoWriter writer = new PicoWriter();
    private boolean hasCountOrder;
    private final AggregateProcessFunction aggregateProcessFunction;

    MultipleAggregateTypeWriter(AggregateProcessFunction apf){
        this.className = "QMultipleAggregateType";
        this.aggregateProcessFunction = apf;

    }



    public String write(String filePath) throws Exception {
        addCaseClassParameters(writer);
        addOpenBrace(writer);
        addAdditionMethod(writer);
        addSubtractionMethod(writer);
        addCloseBrace(writer);
        writeClassFile(this.className, filePath, writer.toString());
        return this.className;
    }

    @Override
    public void addImports(PicoWriter writer) {

    }

    @Override
    public void addConstructorAndOpenClass(PicoWriter writer) {

    }


    void addCaseClassParameters(final PicoWriter writer) throws Exception {
        writer.writeln("case class " + this.className + "(");
        for(AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            String aggregateValueName = aggregateValue.getName().toUpperCase();
            String aggregateValueType = aggregateValue.getValueType().getSimpleName();
            if (aggregateValueType.equals("Integer")) {
                aggregateValueType = "Int";
            }
            writer.writeln("  " + aggregateValueName + " : " + aggregateValueType + ", ");
        }
        writer.write("  cnt : Int");
        writer.writeln(")");
    }


    void addAdditionMethod(final PicoWriter writer) throws Exception {
        writer.writeln("  def +(that : " + this.className + ") : " + this.className + " = {");
        writer.writeln("    " + this.className + "(");
        for(AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            String aggregateValueName = aggregateValue.getName().toUpperCase();
            if (aggregateValue.getAggregation() == Operator.AVG) {
                writer.writeln("      " + aggregateValuesAverageOperations(aggregateValueName, "+"));
            } else {
                writer.writeln("      " + aggregateValuesFirstLevelOperations(aggregateValueName, "+"));
            }
        }
        writer.writeln("      this.cnt + that.cnt");
        writer.writeln("    )");
        writer.writeln("  }");
    }

    void addSubtractionMethod(final PicoWriter writer) throws Exception {
        writer.writeln("  def -(that : " + this.className + ") : " + this.className + " = {");
        writer.writeln("    " + this.className + "(");
        for(AggregateValue aggregateValue : aggregateProcessFunction.getAggregateValues()) {
            String aggregateValueName = aggregateValue.getName().toUpperCase();
            if (aggregateValue.getAggregation() == Operator.AVG) {
                writer.writeln("      " + aggregateValuesAverageOperations(aggregateValueName, "-"));
            } else {
                writer.writeln("      " + aggregateValuesFirstLevelOperations(aggregateValueName, "-"));
            }
        }
        writer.writeln("      this.cnt - that.cnt");
        writer.writeln("    )");
        writer.writeln("  }");
    }

    void addToStringMethod(final PicoWriter writer) throws Exception {

    }

    void addOpenBrace(final PicoWriter writer) throws Exception {
        writer.writeln("{");
    }

    void addCloseBrace(final PicoWriter writer) throws Exception {
        writer.writeln("}");
    }

    String addAggregationValueBinaryOperation(AggregateValue aggregateValue, String op) throws Exception{
        String aggregateValueName = aggregateValue.getName().toUpperCase();

        StringBuilder sb = new StringBuilder();
        if (op.equals("+")) {
            return aggregateValuesFirstLevelOperations(aggregateValueName, "+");

        }
        else if (op.equals("-")) {
            sb.append("this.")
                    .append(aggregateValueName)
                    .append(" - ")
                    .append("that.")
                    .append(aggregateValueName)
                    .append(", ");
        } else {
            throw new Exception("Unsupported Aggregation Value Binary Operation " + op.toString() +" !");
        }

        sb.append("(this.")
                .append(aggregateValueName)
                .append(" * this.cnt ")
                .append("+")
                .append(" that.")
                .append(aggregateValueName)
                .append(" * that.cnt) / (this.cnt + that.cnt),");

        return sb.toString();
    }

    String aggregateValuesFirstLevelOperations(String aggregateValueName, String op) {
        StringBuilder sb = new StringBuilder();
        sb.append("this.")
                .append(aggregateValueName)
                .append(" ")
                .append(op)
                .append(" that.")
                .append(aggregateValueName)
                .append(", ");
        return sb.toString();
    }

    String aggregateValuesAverageOperations(String aggregateValueName, String op) {
        StringBuilder sb = new StringBuilder();
        sb.append("(this.")
                .append(aggregateValueName)
                .append(" * this.cnt ")
                .append(op)
                .append(" that.")
                .append(aggregateValueName)
                .append(" * that.cnt) / (this.cnt ")
                .append(op)
                .append(" that.cnt),");
        return sb.toString();
    }
}
