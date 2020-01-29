package Jobs;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import InputFormats.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MapSideJoin {

    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> C0_data = new HashMap<String, String>();
        private BufferedReader brReader;

        enum MYCOUNTER {
            RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
        }

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            setupOrderHashMap(new Path(Constants.WORD_COUNT_C0_OUTPUT + "/part-r-00000"), context);
        }

        private void setupOrderHashMap(Path filePath, Context context)
                throws IOException {

            String strLineRead = "";

            try {
                brReader = new BufferedReader(new FileReader(filePath.toString()));

                while ((strLineRead = brReader.readLine()) != null) {
                    String c0[] = strLineRead.split("\t");
                    C0_data.put(c0[0],c0[1]);
                }
                Constants.printDebug(C0_data.toString());

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if (brReader != null) {
                    brReader.close();
                }

            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] splitted = value.toString().split("\t");
            String ngram = splitted[0];
            String values = value.toString().substring(value.toString().indexOf('\t')+1);
            context.write(new Text(ngram), new Text(values + '\t' + C0_data.get("C0")));


        }


    }



    /* Create a job instance for N1, N2, N3, C1, C2 */
    private static Job CreateJoinJob(String jobName, String outputDirName, String inputPath1) throws IOException {

        Constants.clearOutput(outputDirName);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(MapSideJoinMapper.class);
        job.setMapperClass(MapSideJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath1));
        FileOutputFormat.setOutputPath(job,new Path(outputDirName));

        return job;
    }


    public static Job[] createJoinTable() {
        Job job_join_C0=null;

        try {
            // The reducer join output file will be from the following format ((w1,w2,w3) N3 N1 N2 C1 C2)

            job_join_C0 = CreateJoinJob(Constants.JOB_JOIN_C0, Constants.JOIN_OUTPUT4, Constants.JOIN_OUTPUT3);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Job [] jobs = {job_join_C0};
        return jobs;
    }


}

