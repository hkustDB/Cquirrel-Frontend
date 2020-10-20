import org.ainslec.picocog.PicoWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@RunWith(MockitoJUnitRunner.class)
public class RelationProcessFunctionWriterTest {

    MockedStatic<RelationSchema> mockSchema = Mockito.mockStatic(RelationSchema.class);

    @Before
    public void initialization() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void q6() throws Exception {
        Node node = JsonParser.parse(new File("src" + File.separator + "test" + File.separator + "resources").getAbsolutePath() + File.separator + "Q6.json");
        RelationProcessFunctionWriter rpfw = new RelationProcessFunctionWriter(node.getRelationProcessFunction());
        rpfw.write("src" + File.separator + "test" + File.separator + "resources");
    }

    @Test
    public void addIsValidFunctionTest() {
        RelationProcessFunctionWriter rpfw = new RelationProcessFunctionWriter();
        List<Value> values = new ArrayList<>();
        String attributeName = "attributeValue";
        values.add(ConstantValue.newInstance("1", "int"));
        values.add(new AttributeValue(attributeName));
        SelectCondition s1 = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        PicoWriter picoWriter = new PicoWriter();
        RelationSchema.Attribute mockAttribute = new RelationSchema.Attribute(Integer.class, 0, attributeName);
        mockSchema.when(() -> RelationSchema.getColumnAttribute(any(String.class))).thenReturn(mockAttribute);
        rpfw.addIsValidFunction(Collections.singletonList(s1), picoWriter);
        System.out.println(picoWriter.toString());

        values.clear();
        values.add(new AttributeValue(attributeName));
        values.add(ConstantValue.newInstance("1", "int"));
        s1 = new SelectCondition(new Expression(values, Operator.LESS_THAN), Operator.AND);
        picoWriter = new PicoWriter();
        mockSchema.when(() -> RelationSchema.getColumnAttribute(any(String.class))).thenReturn(mockAttribute);
        rpfw.addIsValidFunction(Collections.singletonList(s1), picoWriter);
        System.out.println(picoWriter.toString());
    }

    //@AfterEach
    void message() {
        System.out.println("inspect the generated class manually at the specified path and remove it");
    }
}
