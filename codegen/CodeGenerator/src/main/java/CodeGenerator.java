import java.io.File;
import java.io.IOException;

public interface CodeGenerator {
    public void generateCode(final String filePath) throws IOException;
}
