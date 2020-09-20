public class AttributeValue implements Value {

    private final String name;

    public AttributeValue(String name) {
        CheckerUtils.checkNullOrEmpty(name,"name");
        this.name = name;
    }

    @Override
    public String toString() {
        return "AttributeValue{" +
                "name='" + name + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }
}