import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

public class DateValue extends ConstantValue {
    private final Date value;


    public DateValue(String value, DataType type) throws ParseException {
        super(type);
        // TODO please move the data_format to a public util to reduce the memory usage.  Also, the exception is not
        //      handled.
        DateFormat date_format = new java.text.SimpleDateFormat("yyyy-MM-dd");
        this.value = date_format.parse(value);
    }

    public Date getValue() {
        return this.value;
    }
}
