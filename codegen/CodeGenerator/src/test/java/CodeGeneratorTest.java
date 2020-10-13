import org.junit.jupiter.api.Test;

import java.io.File;

public class CodeGeneratorTest {

    @Test
    void q6() throws Exception {
        CodeGenerator.generate(new File("src"+ File.separator +"test"+ File.separator +"resources").getAbsolutePath() + File.separator + "Q6.json",
                "src"+ File.separator +"test"+ File.separator +"resources",
                "file:///home/data/qwangbp/lineitem.tbl",
                "file:///home/data/qwangbp/testQ6.out");
    }
}
