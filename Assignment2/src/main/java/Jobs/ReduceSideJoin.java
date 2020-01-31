package Jobs;

import java.io.IOException;

import InputFormats.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceSideJoin {


    public static class MapClass extends Mapper<Text, TaggedValue, TaggedKey, TaggedValue> {

        @Override
        public void map(Text key, TaggedValue value, Context context) throws IOException, InterruptedException {

            TaggedKey newKey = new TaggedKey(key, value.getTag());
            context.write(newKey, value);
            //Constants.printDebug("newKey: "+newKey.toString());
            //Constants.printDebug("map - tagged key: "+ key + ", tag: " + value.getTag() + ", initial key: "+value.getInitialKey());
        }
    }


    public static class ReduceClass extends Reducer<TaggedKey, TaggedValue, Text, Text> {

        Text currentTag = null;
        Text currentKey = null;
        Text OccValue = null;
        boolean writeMode = false;

        @Override
        public void reduce(TaggedKey taggedKey, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
            //Constants.printDebug("reduce: key-"+taggedKey.toString());
            //Constants.printDebug("OccValue=" + OccValue + ", writeMode="+writeMode+", currentTag="+currentTag+", currentKey="+currentKey);
            if (currentKey == null || !currentKey.equals(taggedKey.getKey())) {
                //Constants.printDebug("1 if");
                OccValue = null;
                writeMode = false;
            } else {
                //Constants.printDebug("2 if");
                writeMode = (currentTag != null && !currentTag.equals(taggedKey.getTag()));
            }

            if (writeMode) {
                //Constants.printDebug("3 if");
                crossProduct(values, context);
            }
            else {
                //Constants.printDebug("4 if");
                //Constants.printDebug("values:");
                for (TaggedValue value : values) {//values will contain 1 value
                    OccValue = new Text(value.getValue());
                    //Constants.printDebug("\t" +value);
                }
            }

            currentTag = new Text(taggedKey.getTag());
            currentKey = new Text(taggedKey.getKey());

            //Constants.printDebug("changed currKey to: "+currentKey+", and currTag to: "+currentTag);
        }

        private void crossProduct(Iterable<TaggedValue> table2Values, Context context) throws IOException, InterruptedException {
            // This specific implementation of the cross product, combine the data of the customers and the orders (
            // of a given costumer id).
            //Constants.printDebug("crossProduct:");
            for (TaggedValue table2Value : table2Values) {
                //Constants.printDebug("\tinit key: " +table2Value.getInitialKey());
                //Constants.printDebug("\tvalue: " +table2Value.getValue());
                context.write(
                        new Text(table2Value.getInitialKey()),
                        new Text(table2Value.getValue().toString() + "\t" + OccValue.toString()));

                //Constants.printDebug("key: " + table2Value.getInitialKey() +", "+table2Value.getValue().toString() + "\t" + OccValue.toString());
            }

        }
    }

    public static class PartitionerClass extends Partitioner<TaggedKey, TaggedValue> {
        // ensure that keys with same key are directed to the same reducer
        @Override
        public int getPartition(TaggedKey key, TaggedValue value, int numPartitions) {
            Constants.printDebug("partition plz");
            return key.getKey().hashCode() % numPartitions;
        }
    }


    /* Create a job instance for N1, N2, N3, C1, C2 */
    private static Job CreateJoinJob(String jobName, String outputDirName, String inputPath1, InputFormat formatToJoin1,
                                     String inputPath2, InputFormat formatToJoin2) throws IOException {

        Constants.clearOutput(outputDirName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ReduceSideJoin.class);
//        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReduceClass.class);


        job.setMapOutputKeyClass(TaggedKey.class);
        job.setMapOutputValueClass(TaggedValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(inputPath1));
        MultipleInputs.addInputPath(job, new Path(inputPath1), formatToJoin1.getClass(), MapClass.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), formatToJoin2.getClass(), MapClass.class);
        FileOutputFormat.setOutputPath(job, new Path(outputDirName));



        return job;
    }


    public static Job[] createJoinTable() {
        Job job_join_N1=null, job_join_N2=null, job_join_C1=null, job_join_C2=null;

        try {
            // The reducer join output file will be from the following format ((w1,w2,w3) N3 N1 N2 C1 C2)

            job_join_N1 = CreateJoinJob(Constants.JOB_JOIN_N1, Constants.JOIN_OUTPUT, Constants.OCC_3_GRAMS_OUTPUT,
                    new InputFormat_w3(), Constants.OCC_1_GRAMS_OUTPUT, new InputFormat_w1());

            job_join_N2 = CreateJoinJob(Constants.JOB_JOIN_N2, Constants.JOIN_OUTPUT1, Constants.JOIN_OUTPUT,
                    new InputFormat_w2_w3(), Constants.OCC_2_GRAMS_OUTPUT, new InputFormat_w1_w2_occ2());

            job_join_C1 = CreateJoinJob(Constants.JOB_JOIN_C1, Constants.JOIN_OUTPUT2, Constants.JOIN_OUTPUT1,
                    new InputFormat_w2(), Constants.OCC_1_GRAMS_OUTPUT, new InputFormat_w1());

            job_join_C2 = CreateJoinJob(Constants.JOB_JOIN_C2, Constants.JOIN_OUTPUT3, Constants.JOIN_OUTPUT2,
                    new InputFormat_w1_w2_occ3(), Constants.OCC_2_GRAMS_OUTPUT, new InputFormat_w1_w2_occ2());

        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Job [] jobs = {job_join_N1, job_join_N2, job_join_C1, job_join_C2};
        return jobs;
    }


}

