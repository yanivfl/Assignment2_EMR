package Jobs;

import java.io.IOException;

import InputFormats.InputFormat_N1;
import InputFormats.InputFormat_N3;
import InputFormats.TaggedKey;
import InputFormats.TaggedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceSideJoin {


    public static class MapClass extends Mapper<LongWritable, Text, TaggedKey, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            TaggedKey tag_key = new TaggedKey();
            tag_key.setKey(value);
            context.write(tag_key, value);
//            Constants.printDebug("map - tagged key: "+ key + ", " + value.getTag());
            Constants.printDebug("map - tagged key: ");
        }
    }


    public static class ReduceClass extends Reducer<TaggedKey, Text, Text, Text> {

//        Object currentTag = null;
//        WritableComparable currentKey = null;
//        TaggedValue OccValue = null;
//        boolean writeMode = false;

        @Override
        public void reduce(TaggedKey taggedKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values){
                context.write(taggedKey.getKey(), value);
            }

//            if (currentKey == null || !currentKey.equals(taggedKey.getKey())) {
//                OccValue = null;
//                writeMode = false;
//            } else
//                writeMode = (currentTag != null && !currentTag.equals(taggedKey.getTag()));
//
//            if (writeMode)
//                crossProduct(values, context);
//            else {
//                for (TaggedValue value : values) //values will contain 1 value
//                    OccValue = value;
//            }
//
//            currentTag = taggedKey.getTag();
//            currentKey = taggedKey.getKey();
        }

//        private void crossProduct(Iterable<TaggedValue> table2Values, Context context) throws IOException, InterruptedException {
//            // This specific implementation of the cross product, combine the data of the customers and the orders (
//            // of a given costumer id).
//            for (TaggedValue table2Value : table2Values) {
//                context.write(
//                        new Text(table2Value.getInitialKey()),
//                        new Text(table2Value.getValue().toString() + "\t" + OccValue.getValue().toString()));
//
//                Constants.printDebug("key: " + table2Value.getInitialKey() +", "+table2Value.getValue().toString() + "\t" + OccValue.getValue().toString());
//            }
//
//        }
    }

    public static class PartitionerClass extends Partitioner<TaggedKey, Text> {
        // ensure that keys with same key are directed to the same reducer
        @Override
        public int getPartition(TaggedKey key, Text value, int numPartitions) {
            return key.getKey().toString().hashCode() % numPartitions;
        }
    }


    /* Create a job instance for N1, N2, N3, C1, C2 */
    private static Job CreateJoinJob(String jobName, String outputDirName, String inputPath1, String inputPath2,
                                     InputFormat formatToJoin) throws IOException {

        // TODO: this may cause bugs!!
        Constants.clearOutput(outputDirName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ReduceSideJoin.class);
//        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReduceClass.class);


        job.setMapOutputKeyClass(TaggedKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(inputPath1));
        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, MapClass.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, MapClass.class);
        FileOutputFormat.setOutputPath(job, new Path(outputDirName));



        return job;
    }


    public static Job[] createJoinTable() {
        Job job_join_N1=null;

        try {
            job_join_N1 = CreateJoinJob(Constants.JOB_JOIN_N1, Constants.JOIN_OUTPUT, Constants.OCC_3_GRAMS_OUTPUT, Constants.OCC_1_GRAMS_OUTPUT, new InputFormat_N1());
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Job [] jobs = {job_join_N1};
        return jobs;
    }


}

