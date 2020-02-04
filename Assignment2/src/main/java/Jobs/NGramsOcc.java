package Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;

public class NGramsOcc {

    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            if(splitted.length < 3){
                Constants.printDebug("Line is different than expected!");
                Constants.printDebug("Text value is: " + value.toString());
                return;
            }
            String ngram = splitted[0];
            String occurrences = splitted[2];
            context.write(new Text(ngram), new IntWritable(Integer.parseInt(occurrences)));
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text,IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }

    }


    /* Create a job instance for JOB_1_GRAM, N2, N3, C1, C2 */
    private static Job CreateCounterJob(String jobName, String outputDirName, String inputPath) throws IOException {

        Constants.clearOutput(outputDirName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(NGramsOcc.class);
        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputDirName));
        return job;
    }


    public static Job[] createOccJobs(String oneGram, String twoGram, String threeGram) {
        Job job_1gram=null, job_2gram=null, job_3gram=null;

        try {
            job_1gram = CreateCounterJob(
                    Constants.JOB_1_GRAM,
                    Constants.getS3OutputPath(Constants.OCC_1_GRAMS_OUTPUT),
                    oneGram);

            job_2gram = CreateCounterJob(
                    Constants.JOB_2_GRAM,
                    Constants.getS3OutputPath(Constants.OCC_2_GRAMS_OUTPUT),
                    twoGram);

            job_3gram = CreateCounterJob(
                    Constants.JOB_3_GRAM,
                    Constants.getS3OutputPath(Constants.OCC_3_GRAMS_OUTPUT),
                    threeGram);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Job [] jobs = {job_1gram, job_2gram, job_3gram};
        Constants.printDebug("OCC Jobs created successfully");
        return jobs;
    }
}
