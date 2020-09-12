public class AttributeValue implements Value {

    private final String name;
    private final String type;

    public AttributeValue(String name, String type) {
        CheckerUtils.checkNullOrEmpty(name,"name");
        CheckerUtils.checkNullOrEmpty(type,"type");
        this.name = name;
        this.type = type;
    }
}