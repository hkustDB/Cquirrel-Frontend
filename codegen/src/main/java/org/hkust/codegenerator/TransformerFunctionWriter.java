package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.Expression;
import org.hkust.objects.TransformerFunction;
import org.hkust.objects.Type;
import org.hkust.schema.RelationSchema;

class TransformerFunctionWriter extends ProcessFunctionWriter {
    private final PicoWriter writer = new PicoWriter();
    private final TransformerFunction transformerFunction;
    private final String outputType;
    private final String className;
    private final RelationSchema relationSchema;

    TransformerFunctionWriter(final TransformerFunction transformerFunction, RelationSchema schema) {
        super(schema);
        this.relationSchema = schema;
        this.transformerFunction = transformerFunction;
        Class<?> type = Double.class;//
        outputType = type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName();
        className = getProcessFunctionClassName(transformerFunction.getName());
    }

    @Override
    public String write(String filePath) throws Exception {
        addImports(writer);
        addConstructorAndOpenClass(writer);
        addExprFunction(writer);


        return className;
    }

    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import org.hkust.RelationType.Payload");
        writer.writeln("import org.hkust.BasedProcessFunctions.TransformerProcessFunction");
    }

    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        //TODO: apply the next_key optimization on thiskey, remember: next_key is now output_key and requires no such optimizations
        String code = "class " +
                "transformer" +
                " extends TransformerProcessFunction[Any, " +
                outputType +
                "](" +
                " aggregateName = \"" +
                className + "\"" +
                ", deltaOutput = true" +
                ") {";
        writer.writeln_r(code);
    }

    @VisibleForTesting
    void addExprFunction(final PicoWriter writer) throws Exception {
        writer.writeln_r("override def expr(value: Payload): " + outputType + " = {");
        Expression expression = transformerFunction.getExpr();
        StringBuilder code = new StringBuilder();
        expressionToCode(expression, code);
        writer.writeln(code.toString());
        writer.writeln_l("}");
    }



}