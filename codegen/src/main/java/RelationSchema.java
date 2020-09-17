import java.util.Collection;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RelationSchema {
    private final String name;
    private final Map<String, Attribute> columns;

    public RelationSchema(String name, Map<String, Attribute> columns) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        CheckerUtils.checkNullOrEmpty((Collection) columns, "columns");
        this.name = name;
        this.columns = columns;
    }

    public class Attribute {
        private final Class type;
        private final int position;

        public Attribute(Class type, int position) {
            requireNonNull(type);
            if (position < 0) {
                throw new RuntimeException("position in Attribute cannot be < 0");
            }
            this.type = type;
            this.position = position;
        }
    }
}
