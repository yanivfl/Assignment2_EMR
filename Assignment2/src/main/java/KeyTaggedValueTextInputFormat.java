import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class KeyTaggedValueTextInputFormat extends FileInputFormat<Text, TaggedValue<Text,Text>> {

    // An implementation of an InputFormat of a text key and text tagged value
    @Override
    public RecordReader<Text, TaggedValue<Text,Text>>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        return new KeyTaggedValueLineRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}