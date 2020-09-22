import java.io.File;
import java.io.IOException;

public interface ProcessFunctionWriter {
    public void generateCode(final String filePath) throws IOException;
}
