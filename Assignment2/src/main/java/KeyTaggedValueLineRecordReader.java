import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class KeyTaggedValueLineRecordReader extends RecordReader<Text, TaggedValue<Text,Text>> {

    // This record reader parses the input file into pairs of text key, and tagged value text,
    // where the tag is based on the name of the input file
    private KeyValueTextRecordReader reader;
    private Text tag;

    public KeyTaggedValueLineRecordReader() {
        super();
        reader = new KeyValueTextRecordReader();
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        reader.initialize(genericSplit, context);
        tag = new Text(((FileSplit)genericSplit).getPath().getName().split("-")[0]);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        return reader.nextKeyValue();
    }

    @Override
    public TaggedValue<Text,Text> getCurrentValue() throws IOException, InterruptedException {
        return new TextTaggedValue(tag,reader.getCurrentValue());
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}