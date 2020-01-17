import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public abstract class OccTaggedValueRecordReader extends RecordReader<Text,TaggedValue> {

    protected LineRecordReader reader;
    protected TaggedValue value;

    private boolean w1;
    private boolean w2;
    private boolean w3;


    OccTaggedValueRecordReader(boolean w1, boolean w2, boolean w3, Text tag) {
        this.w1 = w1;
        this.w2 = w2;
        this.w3 = w3;
        reader = new LineRecordReader();
        value = new TaggedValue(tag);
    }


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        reader.initialize(split, context);
    }


    @Override
    public void close() throws IOException {
        reader.close();
    }


    @Override
    public boolean nextKeyValue() throws IOException {
        return reader.nextKeyValue();
    }


    @Override
    public Text getCurrentKey() {
        return parseKey(reader.getCurrentValue().toString());
    }


    @Override
    public TaggedValue getCurrentValue() throws IOException {
        return parseValue(reader.getCurrentValue().toString());
    }


    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }


    protected Text parseKey(String str) {

        // line structure: ngram \t .....
        String[] ngram = str
                .substring(0, str.indexOf('\t'))
                .split(" ");

        String key = "";
        if (w1)
            key += ngram[0];
        if (w2)
            key += ngram[2];
        if (w3)
            key += ngram[3];

        return new Text(key);
    }


    protected TaggedValue parseValue(String str) throws IOException {

        String initial_key = str.substring( 0, str.indexOf('\t'));
        String value = str.substring(str.indexOf('\t')+1);

        // line structure: ngram \t v1 v2 v3...

        if (this.value.getTag().equals(Constants.TAG_3_OCC)){
            this.value.setInitialKey(new Text(initial_key));
        }

        this.value.setValue(new Text(value));
        return this.value;
    }

}




