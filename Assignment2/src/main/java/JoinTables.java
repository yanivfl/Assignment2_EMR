//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//
//public class JoinTables {
//
//    // This program gets two types of values and produce a join-by-key value sets
//    public static class MapClass
//            extends Mapper<JoinApps.JoinableKey,TaggedValue,WritableComparable,TaggedValue> {
//        // The map gets a key and tagged value (of 2 types) and emits the key and the value
//        @Override
//        public void map(JoinableKey key, TaggedValue taggedValue, Context context) throws IOException,  InterruptedException {
//            context.write(key.getJoinKey(), taggedValue);
//        }
//    }
//
//
//    public static class ReduceClass  extends Reducer<WritableComparable,TaggedValue,Text,Text> {
//        @Override
//        public void reduce
//                (WritableComparable key,Iterable<TaggedValue> taggedValues, Context context) throws IOException, InterruptedException {
//            // The reduce gets a key and a set of values of two types (identified by their tags)
//            // and generates a cross product of the two types of values
//            Map <String,List<Writable>> mapTag2Values = new HashMap<String,List<Writable>>();
//            for (TaggedValue taggedValue : taggedValues) {
//                List<Writable> values = mapTag2Values.get(taggedValue.getTag());
//                if (values == null) {
//                    values = new LinkedList<Writable>();
//                    mapTag2Values.put(taggedValue.getTag(),values);
//                }
//                values.add(taggedValue.getValue());
//            }
//            crossProduct(key,mapTag2Values,context);
//        }
//
//
//        protected void crossProduct(WritableComparable key,Map<String,List<Writable>> mapTag2Values,Context context) throws IOException, InterruptedException {
//            // This specific implementation of the cross product, combine the data of the first tag with each one of the second.
//            // (The Comparable implementation of the Tag controls the order of the tables data for this case)
//            Iterator<List<Writable>> tablesDataIterator = mapTag2Values.values().iterator();
//            List<Writable> table1Data = tablesDataIterator.next();
//            List<Writable> table2Data = tablesDataIterator.next();
//
//            // combine each item of table 2 with the concatenated items of table 1
//            for (Writable action : table2Data)
//                context.write(new Text(key.toString()), new Text(table1Data.toString() + ": " + action.toString()));
//        }
//    }
//}