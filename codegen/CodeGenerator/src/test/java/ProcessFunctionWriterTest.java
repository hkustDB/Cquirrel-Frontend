import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)

public class ProcessFunctionWriterTest {

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Test
    public void expressionToCodeTest() {
        List<Value> values = new ArrayList<>();
        //Dummy data types to test the different code flows, they don't make sense otherwise
        values.add(new ConstantValue("constant1", "date"));
        values.add(new ConstantValue("constant2", "int"));
        values.add(new AttributeValue("attributeName"));
        Expression expression = new Expression(values, Operator.AND);

        StringBuilder code = new StringBuilder();
        testExpressionToCode(expression, code);
        assertEquals(code.toString().replaceAll("\\s+", ""), ("format.parse(\"constant1\")&&constant2&&value(\"ATTRIBUTENAME\").asInstanceOf[Integer]").replaceAll("\\s+", ""));

        //must get a new reference, do not clear the existing values list and reuse the same reference, will result in stack overflow
        values = new ArrayList<>();
        values.add(new ConstantValue("constant2", "int"));
        values.add(expression);
        Expression expression2 = new Expression(values, Operator.OR);
        code = new StringBuilder();
        testExpressionToCode(expression2, code);

        assertEquals(code.toString().replaceAll("\\s+", ""), ("constant2||format.parse(\"constant1\")&&constant2&&value(\"ATTRIBUTENAME\").asInstanceOf[Integer]").replaceAll("\\s+", ""));
    }

    @Test
    public void inValidExpression() {
        List<Value> values = new ArrayList<>();

        values.add(new ConstantValue("constant1", "date"));
        thrownException.expect(IllegalArgumentException.class);
        thrownException.expectMessage("Expression with 1 value can only have ! as the operator");
        new Expression(values, Operator.LESS_THAN);

        values.add(new ConstantValue("constant2", "int"));
        values.add(new AttributeValue("attributeName"));
        thrownException.expect(IllegalArgumentException.class);
        thrownException.expectMessage("Expression with more than 2 values can only have && or || as the operator");
        new Expression(values, Operator.LESS_THAN);

        new Expression(values, Operator.AND);
        new Expression(values, Operator.OR);
    }

    private void testExpressionToCode(Expression expression, StringBuilder code) {
        try (MockedStatic<RelationSchema> mockSchema = Mockito.mockStatic(RelationSchema.class)) {
            RelationSchema.Attribute mockAttribute = new RelationSchema.Attribute(Integer.class, 0, "attributeName");
            mockSchema.when(() -> RelationSchema.getColumnAttribute(any(String.class))).thenReturn(mockAttribute);
            ProcessFunctionWriter.expressionToCode(expression, code);
        }
    }

}
