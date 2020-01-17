import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class TaggedValue implements Writable {

    // An implementation of value with tag, as a writable object

    private Text tag;
    private Text value;
    private Text initialKey;

    TaggedValue() {
        tag = null;
        value = null;
        initialKey = null;
    }

    TaggedValue(Text tag) {
        this.tag = tag;
        this.initialKey = null;
        this.value = null;
    }

    @Override
    public void readFields(DataInput data) throws IOException {
        tag.readFields(data);
        value.readFields(data);
        initialKey.readFields(data);
    }

    @Override
    public void write(DataOutput data) throws IOException {
        tag.write(data);
        value.write(data);
        initialKey.write(data);
    }

    @Override
    public String toString() {
        return "TaggedValue{" +
                "tag=" + tag +
                ", value=" + value +
                ", initialKey=" + initialKey +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        TaggedValue other = (TaggedValue)o;
        return tag.equals(other.tag) && value.equals(other.value);
    }


    public Text getTag() {
        return tag;
    }

    public Text getValue() {
        return value;
    }

    public Text getInitialKey() {
        return initialKey;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    public void setInitialKey(Text initialKey) {
        this.initialKey = initialKey;
    }
}