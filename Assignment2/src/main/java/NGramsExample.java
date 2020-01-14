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

import java.io.IOException;

public class NGramsExample {

    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            String ngram = splitted[0];
//            String year = splitted[1];
            String occurrences = splitted[2];
            String[] ngram_words = ngram.split(" ");
            context.write(new Text(ngram_words[0]+ ' ' + ngram_words[1] + ' ' + ngram_words[2]), new IntWritable(Integer.parseInt(occurrences)));
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

    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = new Job(conf, "word count");
//        job.setJarByClass(NGramsExample.class);
//        job.setMapperClass(MapClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReduceClass.class);
//        job.setReducerClass(ReduceClass.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path("../mini_corpus.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("output_corpus"));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "3rd word count");
        job.setJarByClass(NGramsExample.class);
        job.setMapperClass(NGramsExample.MapClass.class);
        job.setCombinerClass(NGramsExample.ReduceClass.class);
        job.setReducerClass(NGramsExample.ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/home/yaniv/workSpace/dsps/ASS2/Assignment2_dsps/mini_corpus.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output_corpus"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
