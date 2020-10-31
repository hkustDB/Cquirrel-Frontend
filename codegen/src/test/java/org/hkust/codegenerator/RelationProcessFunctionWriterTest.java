package org.hkust.codegenerator;

import org.ainslec.picocog.PicoWriter;
import org.hkust.objects.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RelationProcessFunctionWriterTest {

    @Mock
    private RelationProcessFunction relationProcessFunction;

    @Before
    public void initialization() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void addConstructorAndOpenClassTest() {
        when(relationProcessFunction.getRelationName()).thenReturn("RelationName");
        when(relationProcessFunction.getThisKey()).thenReturn(Arrays.asList("thisKey1", "thisKey2"));
        when(relationProcessFunction.getNextKey()).thenReturn(Arrays.asList("nextKey1", "nextKey2"));
        PicoWriter picoWriter = new PicoWriter();
        getRelationProcessFunctionWriter().addConstructorAndOpenClass(picoWriter);
        assertEquals(picoWriter.toString().replaceAll("\\s+", ""),
                "class ClassNameProcessFunction extends RelationFKProcessFunction[Any](\"RelationName\",Array(\"thisKey1\",\"thisKey2\"),Array(\"nextKey1\",\"nextKey2\"),true){".replaceAll("\\s+", ""));
    }

    @Test
    public void isValidFunctionWithSingleCondition() {
        List<Value> values = new ArrayList<>();
        String attributeName = "attributeValue";
        values.add(new ConstantValue("1", "int"));
        values.add(new AttributeValue(attributeName));
        SelectCondition condition = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        RelationSchema.Attribute mockAttribute = new RelationSchema.Attribute(Integer.class, 0, attributeName);
        isValidFunctionTest(Collections.singletonList(condition), ("override def isValid(value: Payload): Boolean = {\n" +
                        "   if(1<value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]){\n" +
                        "   true}else{\n" +
                        "   false}\n" +
                        "}").replaceAll("\\s+", ""),
                mockAttribute);

        values.clear();
        values.add(new AttributeValue(attributeName));
        values.add(new ConstantValue("1", "int"));
        condition = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        isValidFunctionTest(Collections.singletonList(condition), ("override def isValid(value: Payload): Boolean = {\n" +
                        "   if(value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]<1){\n" +
                        "   true}else{\n" +
                        "   false}\n" +
                        "}").replaceAll("\\s+", ""),
                mockAttribute);
    }

    @Test
    public void isValidFunctionWithMultipleConditions() {
        List<Value> values = new ArrayList<>();
        String attributeName = "attributeValue";
        values.add(new ConstantValue("1", "int"));
        values.add(new AttributeValue(attributeName));
        SelectCondition condition1 = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        SelectCondition condition2 = new SelectCondition(new Expression(values, Operator.EQUALS), Operator.AND);
        RelationSchema.Attribute mockAttribute = new RelationSchema.Attribute(Integer.class, 0, attributeName);
        isValidFunctionTest(Arrays.asList(condition1, condition2),
                "overridedefisValid(value:Payload):Boolean={if(1<value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]&&1=value(\"ATTRIBUTEVALUE\").asInstanceOf[Integer]){true}else{false}}",
                mockAttribute);
    }

    private void isValidFunctionTest(List<SelectCondition> selectConditions, final String expectedCode, RelationSchema.Attribute mockAttribute) {
        PicoWriter picoWriter = new PicoWriter();
        try (MockedStatic<RelationSchema> mockSchema = Mockito.mockStatic(RelationSchema.class)) {
            mockSchema.when(() -> RelationSchema.getColumnAttribute(any(String.class))).thenReturn(mockAttribute);
            getRelationProcessFunctionWriter().addIsValidFunction(selectConditions, picoWriter);
        }
        assertEquals(picoWriter.toString().replaceAll("\\s+", ""), expectedCode);
    }

    private RelationProcessFunctionWriter getRelationProcessFunctionWriter() {
        when(relationProcessFunction.getName()).thenReturn("ClassName");
        return new RelationProcessFunctionWriter(relationProcessFunction);
    }
}
