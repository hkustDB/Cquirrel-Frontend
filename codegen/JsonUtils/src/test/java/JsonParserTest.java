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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.internal.LinkedTreeMap;


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
        when(mockMap.get("select_conditions")).thenReturn(new HashMap<>());
        try (MockedStatic<JsonParser> mock = Mockito.mockStatic(JsonParser.class)) {
            mock.when(() -> JsonParser.makeSelectConditions(any())).thenReturn(Collections.singletonList(new SelectCondition(
                    new Expression(Arrays.asList(new AttributeValue("attributeValue1"), new AttributeValue("attributeValue2")), Operator.AND),
                    Operator.AND
            )));
            requireNonNull(JsonParser.makeRelationProcessFunction(mockMap));
        }
    }

    @Test
    public void makeAggregateProcessFunctionTest() throws Exception {
        when(mockMap.get("name")).thenReturn("AggregateProcessFunction");
        when(mockMap.get("this_key")).thenReturn(Collections.singletonList("this_key"));
        when(mockMap.get("next_key")).thenReturn(Collections.singletonList("next_key"));
        when(mockMap.get("aggregation")).thenReturn("*");
        when(mockMap.get("value_type")).thenReturn("Double");
        when(mockMap.get("AggregateValue")).thenReturn(new HashMap<>());
        try (MockedStatic<JsonParser> mock = Mockito.mockStatic(JsonParser.class)) {
            mock.when(() -> JsonParser.makeAggregateValue(any())).thenReturn(Collections.singletonList(
                    new AggregateProcessFunction.AggregateValue("AggregateValue",
                            "expression",
                            new AttributeValue("attributeValue"))
            ));
            requireNonNull(JsonParser.makeAggregateProcessFunction(mockMap));
        }
    }

    @Test
    public void makeAggregateValueTest() {
        when(mockMap.get("type")).thenReturn("expression");
        when(mockMap.get("name")).thenReturn("AggregateValue");
        when(mockMap.get("operator")).thenReturn("*");
        when(mockMap.entrySet()).thenReturn(new HashSet<>(Arrays.asList(
                getEntry("value1", "attribute"),
                getEntry("value2", "constant")
        )));

        List<AggregateProcessFunction.AggregateValue> result = JsonParser.makeAggregateValue(mockMap);
        assertEquals(result.size(), 1);
        AggregateProcessFunction.AggregateValue aggregateValue = result.get(0);
        Value value = aggregateValue.getValue();
        assertTrue(value instanceof Expression);
        Expression expression = (Expression) value;
        assertEquals(expression.getValues().size(), 2);

        JsonParser.makeAggregateValue(mockMap);

    }

    @Test
    public void makeAggregateValueExceptionsTest() {
        when(mockMap.get("type")).thenReturn("wrongType");
        thrownException.expect(RuntimeException.class);
        thrownException.expectMessage("Unknown AggregateValue type. Currently only supporting expression type.");
        JsonParser.makeAggregateValue(mockMap);

        when(mockMap.get("type")).thenReturn("expression");
        when(mockMap.get("name")).thenReturn("AggregateValue");
        when(mockMap.get("operator")).thenReturn("*");
        when(mockMap.entrySet()).thenReturn(new HashSet<>());
        thrownException.expect(RuntimeException.class);
        thrownException.expectMessage("List of values supplied to Expression in AggregateValue cannot be empty");
        JsonParser.makeAggregateValue(mockMap);


        when(mockMap.entrySet()).thenReturn(new HashSet<>(Arrays.asList(
                getEntry("value1", "wrongType"),
                getEntry("value2", "constant")
        )));
        thrownException.expect(RuntimeException.class);
        thrownException.expectMessage("Unknown type for AggregateValue");
        JsonParser.makeAggregateValue(mockMap);
    }

    @Test
    public void makeSelectConditionsTest() {
        when(mockMap.get("operator")).thenReturn("<");
        when(mockMap.entrySet()).thenReturn(new HashSet<>(Arrays.asList(
                getEntry("value1", "attribute"))
        ));

        List<SelectCondition> result;
        try (MockedStatic<JsonParser> mock = Mockito.mockStatic(JsonParser.class)) {
            mock.when(() -> JsonParser.makeValue(any())).thenReturn(new AttributeValue("attributeValue"));
            result = JsonParser.makeSelectConditions(mockMap);
        }

        assertEquals(result.size(), 1);
    }

    @NotNull
    private Map.Entry<String, Object> getEntry(String key, String type) {
        return new Map.Entry<String, Object>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Object getValue() {
                LinkedTreeMap map = mock(LinkedTreeMap.class);
                //mocking for AttributeValue, ConstantValue and SelectConditions
                when(map.get("type")).thenReturn(type);
                when(map.get("name")).thenReturn(type + "Name");
                when(map.get("value")).thenReturn(type + "Value");
                when(map.get("var_type")).thenReturn("Date");
                when(map.get("operator")).thenReturn("<");

                return map;
            }

            @Override
            public Object setValue(Object o) {
                return null;
            }
        };
    }
}
