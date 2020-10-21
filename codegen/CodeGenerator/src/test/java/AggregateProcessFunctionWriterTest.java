import org.ainslec.picocog.PicoWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AggregateProcessFunctionWriterTest {

    @Mock
    private AggregateProcessFunction aggregateProcessFunction;

    @Before
    public void initialization() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void addConstructorAndOpenClassTest() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addConstructorAndOpenClass(picoWriter);

        assertEquals(picoWriter.toString().replaceAll("\\s+", ""), ("class ClassNameProcessFunction extends AggregateProcessFunction[Any, Integer](\"ClassNameProcessFunction\", Array(), Array(), aggregateName = \"aggregateName\") {").replaceAll("\\s+", ""));
    }

    @Test
    public void aggregateFunctionTest() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addAdditionFunction(picoWriter);

        assertEquals(picoWriter.toString().replaceAll("\\s+", ""), ("override def addition(value1: Integer, value2: Integer): Integer = value1 + value2").replaceAll("\\s+", ""));
    }

    @Test
    public void additionFunctionTest() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addAdditionFunction(picoWriter);

        assertEquals(picoWriter.toString().replaceAll("\\s+", ""), ("override def addition(value1: Integer, value2: Integer): Integer = value1 + value2").replaceAll("\\s+", ""));
    }

    @Test
    public void subtractionFunctionTest() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addSubtractionFunction(picoWriter);

        assertEquals(picoWriter.toString().replaceAll("\\s+", ""), ("override def subtraction(value1: Integer, value2: Integer): Integer = value1 - value2").replaceAll("\\s+", ""));
    }

    @Test
    public void initStateFunction() {
        PicoWriter picoWriter = new PicoWriter();
        getAggregateProcessFunctionWriter(Integer.class)
                .addInitStateFunction(picoWriter);

        assertEquals(picoWriter.toString().replaceAll("\\s+", ""),("override def initstate(): Unit = {\n" +
                "   val valueDescriptor = TypeInformation.of(new TypeHint[Integer](){})\n" +
                "   val aliveDescriptor : ValueStateDescriptor[Integer] = new ValueStateDescriptor[Integer](\"ClassNameProcessFunction\"+\"Alive\", valueDescriptor)\n" +
                "   alive = getRuntimeContext.getState(aliveDescriptor)\n" +
                "   }\n" +
                "      override val init_value: Integer = 0.0").replaceAll("\\s+", ""));
    }

    private AggregateProcessFunctionWriter getAggregateProcessFunctionWriter(Class<?> aggregateType) {
        when(aggregateProcessFunction.getName()).thenReturn("ClassName");
        when(aggregateProcessFunction.getValueType()).thenReturn(aggregateType);
        AggregateProcessFunction.AggregateValue aggregateValue = new AggregateProcessFunction.AggregateValue("aggregateName", "expression", new AttributeValue("attributeValue"));
        when(aggregateProcessFunction.getAggregateValues()).thenReturn(Collections.singletonList(aggregateValue));
        return new AggregateProcessFunctionWriter(aggregateProcessFunction);
    }
}
