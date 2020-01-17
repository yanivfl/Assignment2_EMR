//import java.io.IOException;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//
//public class ReduceSideJoin {
//
//    // This program gets two types of values and produce a join-by-key value sets
//    public static class MapClass  extends Mapper<Text,TaggedValue<Text,Text>,Text,TaggedValue<Text,Text>> {
//        // The map gets a key and tagged value (of 2 types) and emits the key and the value
//        @Override
//        public void map(Text key, TaggedValue<Text,Text> value, Context context) throws IOException,  InterruptedException {
//            context.write(key, value);
//        }
//    }
//
//
//    public static class ReduceClass  extends Reducer<Text,TaggedValue<Text,Text>,Text,Text> {
//        @Override
//        public void reduce(Text key, Iterable<TaggedValue<Text,Text>> taggedValues, Context context) throws IOException, InterruptedException {
//            // The reduce gets a key and a set of values of two types (identified by their tags)
//            // and generates a cross product of the two types of values
//            Map<Text,List<Text>> mapTag2Values = new HashMap<Text,List<Text>>();
//            for (TaggedValue<Text,Text> taggedValue : taggedValues) {
//                List<Text> values = mapTag2Values.get(taggedValue.getTag());
//                if (values == null) {
//                    values = new LinkedList<Text>();
//                    mapTag2Values.put(taggedValue.getTag(),values);
//                }
//                values.add(taggedValue.getvalue());
//            }
//            crossProduct(key,mapTag2Values,context);
//        }
//
//
//        protected void crossProduct(Text key,Map<Text,List<Text>> mapTag2Values,Context context) throws IOException, InterruptedException {
//            // This specific implementation of the cross product, combine the data of the customers and the orders (
//            // of a given costumer id).
//            StringBuilder sbCustomer = new StringBuilder();
//            for (Text customer : mapTag2Values.get("customers")) {
//                sbCustomer.append(customer.toString());
//                sbCustomer.append(",");
//            }
//            for (Text order : mapTag2Values.get("orders"))
//                context.write(key, new Text(sbCustomer.toString() + order.toString()));
//        }
//    }
//
//
//
//    public static void main(String[] args) throws Exception {
////        Configuration conf = new Configuration();
////        Job job = new Job(conf, "DataJoin");
////        job.setJarByClass(MapperSideJoinWithDistributedCache.class);
////        job.setMapperClass(MapClass.class);
////        job.setReducerClass(ReduceClass.class);
////        FileInputFormat.addInputPath(job, new Path(args[0]));
////        FileOutputFormat.setOutputPath(job, new Path(args[1]));
////        job.setInputFormatClass(KeyTaggedValueTextInputFormat.class);
////        job.setOutputFormatClass(TextOutputFormat.class);
////        job.setOutputKeyClass(Text.class);
////        job.setOutputValueClass(Text.class);
////
////        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}