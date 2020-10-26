import com.google.gson.internal.LinkedTreeMap;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class JsonParserTest {

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Mock
    public Map<String, Object> mockMap;

    @Before
    public void initialization() {
        MockitoAnnotations.openMocks(this);
    }


    @Test
    public void makeRelationProcessFunctionTest() throws Exception {
        when(mockMap.get("name")).thenReturn("RelationProcessFunction");
        when(mockMap.get("relation")).thenReturn("relation");
        when(mockMap.get("this_key")).thenReturn(Collections.singletonList("this_key"));
        when(mockMap.get("next_key")).thenReturn(Collections.singletonList("next_key"));
        when(mockMap.get("child_nodes")).thenReturn(1);
        when(mockMap.get("is_Root")).thenReturn(true);
        when(mockMap.get("is_Last")).thenReturn(false);
        when(mockMap.get("rename_attribute")).thenReturn(null);
        List<SelectCondition> selectConditions = Collections.singletonList(new SelectCondition(
                getExpression(),
                Operator.AND
        ));
        requireNonNull(JsonParser.makeRelationProcessFunction(mockMap, selectConditions));
    }

    @Test
    public void makeAggregateProcessFunctionTest() throws Exception {
        when(mockMap.get("name")).thenReturn("AggregateProcessFunction");
        when(mockMap.get("this_key")).thenReturn(Collections.singletonList("this_key"));
        when(mockMap.get("next_key")).thenReturn(Collections.singletonList("next_key"));
        when(mockMap.get("aggregation")).thenReturn("*");
        when(mockMap.get("value_type")).thenReturn("Double");
        List<AggregateProcessFunction.AggregateValue> aggregateValues = Collections.singletonList(
                new AggregateProcessFunction.AggregateValue("AggregateValue",
                        "expression",
                        new AttributeValue("attributeValue"))
        );
        requireNonNull(JsonParser.makeAggregateProcessFunction(mockMap, aggregateValues));
    }

    @Test
    public void makeAggregateValueTest() {
        when(mockMap.get("type")).thenReturn("expression");
        when(mockMap.get("name")).thenReturn("AggregateValue");

        List<AggregateProcessFunction.AggregateValue> result = JsonParser.makeAggregateValue(mockMap,
                Collections.singletonList(new Expression(Collections.singletonList(new AttributeValue("attributeValue")), Operator.NOT)));
        assertEquals(result.size(), 1);
        AggregateProcessFunction.AggregateValue aggregateValue = result.get(0);
        Value value = aggregateValue.getValue();
        assertTrue(value instanceof Expression);
        Expression expression = (Expression) value;
        assertEquals(expression.getValues().size(), 1);
    }

    @Test
    public void makeAggregateValueExceptionsTest() {
        when(mockMap.get("type")).thenReturn("wrongType");
        thrownException.expect(RuntimeException.class);
        thrownException.expectMessage("Unknown AggregateValue type. Currently only supporting expression type.");
        JsonParser.makeAggregateValue(mockMap, null);
    }

    @Test
    public void makeSelectConditionsTest() {
        when(mockMap.get("operator")).thenReturn("<");
        List<SelectCondition> result = JsonParser.makeSelectConditions(mockMap, Collections.singletonList(getExpression()));
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getExpression().getValues().size(), 2);
    }

    @Test
    public void makeSelectConditionsExpressionsTest() {
        List<Expression> result = JsonParser.makeSelectConditionsExpressions(new HashSet<>(Arrays.asList(
                getEntry("value1"),
                getEntry("value2")
        )));

        assertEquals(result.size(),2);
        assertEquals(result.get(0).getValues().size(), 2);
        assertEquals(result.get(1).getValues().size(), 2);
        assertEquals(result.get(0).getValues(), result.get(1).getValues());
    }

    @NotNull
    private Map.Entry<String, Object> getEntry(String key) {
        return new Map.Entry<String, Object>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Object getValue() {
                LinkedTreeMap map = mock(LinkedTreeMap.class);
                when(map.get("operator")).thenReturn("<");
                when(map.get("left_field")).thenReturn(new HashMap<String, Object>(){
                    {
                        put("type", "attribute");
                        put("name", "attributeName");

                    }
                });

                when(map.get("right_field")).thenReturn(new HashMap<String, Object>(){
                    {
                        put("type", "constant");
                        put("value", "0.07");
                        put("var_type", "Double");

                    }
                });

                return map;
            }

            @Override
            public Object setValue(Object o) {
                return null;
            }
        };
    }

    @NotNull
    private Expression getExpression() {
        return new Expression(Arrays.asList(new AttributeValue("attributeValue1"), new AttributeValue("attributeValue2")), Operator.AND);
    }
}
