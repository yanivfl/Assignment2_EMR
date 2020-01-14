import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class WordCounter {

    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("C0");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, one);
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


    /* Create a job instance for JOB_1_GRAM, N2, N3, C1, C2 */
    private static Job CreateWordCountJob(String jobName, String outputDirName, String inputPath) throws IOException {

        Constants.clearOutput(outputDirName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(WordCounter.class);
        job.setMapperClass(MapClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputDirName));

        return job;
    }


    public static Job createWordCountTable() {

        try {
            return CreateWordCountJob(Constants.JOB_C0, Constants.WORD_COUNT_C0, Constants.OCC_1_GRAMS);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return null;


//        System.exit(job.waitForCompletion(true)? 0 : 1);




    }




}
