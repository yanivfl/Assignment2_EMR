import org.apache.hadoop.io.Text;


public class TextTaggedValue extends TaggedValue<Text,Text> {

    public TextTaggedValue() {
        super();
    }

    public TextTaggedValue(Text tag) {
        super(tag);
    }

    public TextTaggedValue(Text tag,Text value) {
        super(tag,value);
    }

    @Override
    protected void init() {
        tag = new Text();
        value = new Text();
    }

}