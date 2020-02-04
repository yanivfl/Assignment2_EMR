package Jobs;

import InputFormats.ProbabilityKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProbabilityWithSort {

    public static class MapClass extends Mapper<LongWritable, Text, ProbabilityKey, DoubleWritable> {

        private double computeK(int N) {
            double log = Math.log(N + 1);
            return (log + 1)/(log + 2);
        }

        private double computeProbability(int N1, int N2, int N3, int C0, int C1, int C2) {
            double K2 = computeK(N2);
            double K3 = computeK(N3);
            double prod1 = K3 * (N3/C2);
            double prod2 = (1-K3) * K2 * (N2/C1);
            double prod3 = (1-K3) * (1-K2) * (N1/C0);

            return prod1 + prod2 + prod3;

        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            if (splitted.length < 7) {
                Constants.printDebug("ProbabilityWithSort got wrong input, value is: "+ value);
            }

            double probability = computeProbability(
                    Integer.parseInt(splitted[Constants.N1_IDX]),
                    Integer.parseInt(splitted[Constants.N2_IDX]),
                    Integer.parseInt(splitted[Constants.N3_IDX]),
                    Integer.parseInt(splitted[Constants.C0_IDX]),
                    Integer.parseInt(splitted[Constants.C1_IDX]),
                    Integer.parseInt(splitted[Constants.C2_IDX])
            );

            ProbabilityKey newKey = new ProbabilityKey(splitted[Constants.W1_W2_W3_IDX], probability);
            DoubleWritable newValue = new DoubleWritable(probability);
            context.write(newKey, newValue);
        }
    }

    public static class ReduceClass extends Reducer<ProbabilityKey, DoubleWritable, Text, DoubleWritable> {

        public void reduce(ProbabilityKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                context.write(new Text(key.getW1_w2_w3()), new DoubleWritable(value.get()));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<ProbabilityKey,DoubleWritable> {

        @Override
        public int getPartition(ProbabilityKey key, DoubleWritable value, int numPartitions) {
            return Math.abs(key.getW1_w2_w3().hashCode()) % numPartitions;
        }
    }


    /* Create a job instance for JOB_1_GRAM, N2, N3, C1, C2 */
    private static Job CreateCounterJob(String jobName, String outputDirName, String inputPath) throws IOException {

        Constants.clearOutput(outputDirName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ProbabilityWithSort.class);
        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setMapOutputKeyClass(ProbabilityKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputDirName));

        return job;
    }


    public static Job createProbabilityWithSortJob() {
        Job probabilityWithSort = null;

        try {
            probabilityWithSort = CreateCounterJob(
                    Constants.JOB_PROB_WITH_SORT,
                    Constants.getS3OutputPath(Constants.JOIN_OUTPUT5),
                    Constants.getS3OutputPath(Constants.JOIN_OUTPUT4));

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        Constants.printDebug("finished creating probability with sort job");
        return probabilityWithSort;
    }
}
