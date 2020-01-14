//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.InputSplit;
//
//public class JoinApps {
//
//
//    public interface JoinableKey extends WritableComparable<JoinableKey> {
//        WritableComparable getJoinKey();
//    }
//
//    public interface TaggedValue extends Writable {
//        String getTag();
//        Writable getValue();
//    }
//
//    public class TaggedKeyValueTextInputFormat extends FileInputFormat<TaggedKey<Text,Text>, Text> {
//
//        // An implementation of an InputFormat of a text key and text tagged value
//        @Override
//        public RecordReader<TaggedKey<Text,Text>, Text>
//        createRecordReader(InputSplit split,
//                           TaskAttemptContext context) {
//            return new TaggedKeyValueLineRecordReader();
//        }
//
//        @Override
//        protected boolean isSplitable(JobContext context, Path file) {
//            CompressionCodec codec =
//                    new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
//            return codec == null;
//        }
//
//
//    }
//}
