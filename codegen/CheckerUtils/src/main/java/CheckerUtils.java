import java.util.Collection;
import java.util.List;

public class CheckerUtils {
    private static final String NULL_OR_EMPTY_MESSAGE = " cannot be null or empty";

    //TODO: could these functions be made generic?
    public static void checkNullOrEmpty(String string, String name) {
        if (string == null || string.isEmpty()) {
            throw new RuntimeException(name + NULL_OR_EMPTY_MESSAGE);
        }
    }

    public static void checkNullOrEmpty(Collection list, String name) {
        if (list == null || list.isEmpty()) {
            throw new RuntimeException(name + NULL_OR_EMPTY_MESSAGE);
        }
    }
}
