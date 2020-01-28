package InputFormats;

import Jobs.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    // An implementation of key with tag, as a writable object

    private Text tag;
    private Text key;

    public TaggedKey(Text key, Text tag) {
        this.key = new Text(key);
        this.tag = new Text(tag);
    }

    public TaggedKey() {
        this.key = new Text();
        this.tag = new Text();
    }

    @Override
    public void readFields(DataInput data) throws IOException {
        tag.readFields(data);
        key.readFields(data);
    }

    @Override
    public void write(DataOutput data) throws IOException {
        tag.write(data);
        key.write(data);
    }

    @Override
    public String toString() {
        return "TaggedKey{" +
                "tag=" + tag +
                ", key=" + key +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        TaggedKey other = (TaggedKey)o;
        return tag.equals(other.tag) && key.equals(other.key);
    }


    public Text getTag() {
        return tag;
    }

    public Text getKey() {
        return key;
    }


    @Override
    public int compareTo(TaggedKey o) {
        return tag.toString().compareTo(o.getTag().toString());
    }

    public void setKey(Text key) {
        this.key = new Text(key.toString());
    }

    public void setTag(Text tag) {
        this.key = new Text(tag.toString());
    }
}