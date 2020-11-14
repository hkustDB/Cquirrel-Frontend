package org.hkust.codegenerator;

import com.google.common.annotations.VisibleForTesting;
import org.ainslec.picocog.PicoWriter;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.objects.RelationProcessFunction;
import org.hkust.objects.SelectCondition;
import org.hkust.schema.RelationSchema;

import java.util.List;

class RelationProcessFunctionWriter extends ProcessFunctionWriter {
    private final String className;
    private final PicoWriter writer = new PicoWriter();
    private final RelationProcessFunction relationProcessFunction;

    RelationProcessFunctionWriter(final RelationProcessFunction relationProcessFunction, RelationSchema schema) {
        super(schema);
        this.relationProcessFunction = relationProcessFunction;
        this.className = getProcessFunctionClassName(relationProcessFunction.getName());
    }

    /**
     * Meant to be used for testing only
     */

    @Override
    public String write(final String filePath) throws Exception {
        CheckerUtils.checkNullOrEmpty(filePath, "filePath");
        addImports(writer);
        addConstructorAndOpenClass(writer);
        addIsValidFunction(relationProcessFunction.getSelectConditions(), writer);
        closeClass(writer);
        writeClassFile(className, filePath, writer.toString());

        return className;
    }

    @VisibleForTesting
    void addIsValidFunction(List<SelectCondition> selectConditions, final PicoWriter writer) throws Exception {
        writer.writeln_r("override def isValid(value: Payload): Boolean = {");
        StringBuilder ifCondition = new StringBuilder();
        ifCondition.append("if(");
        SelectCondition condition;
        for (int i = 0; i < selectConditions.size(); i++) {
            condition = selectConditions.get(i);
            expressionToCode(condition.getExpression(), ifCondition);
            if (i < selectConditions.size() - 1) {
                //operator that binds each select condition
                ifCondition.append(condition.getOperator().getValue());
            }
        }

        writer.write(ifCondition.toString());
        writer.writeln_r("){");
        writer.write("true");
        writer.writeln_lr("}else{");
        writer.write("false");
        writer.writeln_l("}");
        writer.writeln_l("}");
    }

    @Override
    public void addImports(final PicoWriter writer) {
        writer.writeln("import scala.math.Ordered.orderingToOrdered");
        writer.writeln("import org.hkust.BasedProcessFunctions.RelationFKProcessFunction");
        writer.writeln("import org.hkust.RelationType.Payload");
    }


    @Override
    public void addConstructorAndOpenClass(final PicoWriter writer) {
        boolean isLeaf = relationProcessFunction.isLeaf();
        String code = "class " +
                className +
                (isLeaf ? " extends RelationFKProcessFunction[Any](\"" : " extends RelationFKCoProcessFunction[Any](\"") +
                relationProcessFunction.getRelation().getValue() + "\"," +
                (isLeaf ? "" : relationProcessFunction.getChildNodes() + ",") +
                keyListToCode(relationProcessFunction.getThisKey()) +
                "," +
                keyListToCode(relationProcessFunction.getNextKey()) +
                "," + relationProcessFunction.isRoot()
                + (isLeaf ? ") {" : ", " + relationProcessFunction.isLast() + ") {");
        writer.writeln(code);
    }
}