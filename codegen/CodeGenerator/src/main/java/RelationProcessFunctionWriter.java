import org.ainslec.picocog.PicoWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class RelationProcessFunctionWriter extends ProcessFunctionWriter {
    private final String className;
    private final String relationName;
    private final static PicoWriter writer = new PicoWriter();
    private final RelationProcessFunction relationProcessFunction;

    //TODO: simplify: make methods static, consider giving the generated class a generic name e.g. simply RelationProcessFunction
    public RelationProcessFunctionWriter(final RelationProcessFunction relationProcessFunction) {
        requireNonNull(relationProcessFunction);
        this.relationProcessFunction = relationProcessFunction;
        this.relationName = relationProcessFunction.getName();
        this.className = relationProcessFunction.getName() + "ProcessFunction";
    }

    @Override
    public void generateCode(final String filePath) throws IOException {
        CheckerUtils.checkNullOrEmpty(filePath, "filePath");
        addImports();
        addConstructorAndOpenClass();
        addIsValidFunction(relationProcessFunction.getSelectConditions());
        closeClass(writer);
        writeClassFile(className,filePath,writer.toString());
    }

    private void addIsValidFunction(List<SelectCondition> selectConditions) {
        writer.writeln_r("override def isValid(value: Payload): Boolean = {");
        StringBuilder ifCondition = new StringBuilder();
        ifCondition.append("if(");
        SelectCondition condition;
        for (int i = 0; i < selectConditions.size(); i++) {
            ifCondition.append("value");
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
    public void addImports() {
        writer.writeln("import scala.math.Ordered.orderingToOrdered");
        writer.writeln("import org.hkust.BasedProcessFunctions.RelationFKProcessFunction");
        writer.writeln("import org.hkust.RelationType.Payload");
    }


    @Override
    public void addConstructorAndOpenClass() {
        String code = "class " +
                className +
                " extends RelationFKProcessFunction[Any](\"" +
                relationName + "\"," +
                keyListToCode(relationProcessFunction.getThisKey()) +
                "," +
                keyListToCode(relationProcessFunction.getNextKey()) +
                "," + "true)" + "{";
        writer.writeln(code);
    }
}
