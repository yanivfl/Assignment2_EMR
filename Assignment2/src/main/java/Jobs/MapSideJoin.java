package Jobs;
import java.io.*;
import java.util.HashMap;

import InputFormats.*;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import handlers.S3Handler;
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

        private  static HashMap<String, String> C0_data = new HashMap<String, String>();

        public static void listFilesForFolder(final File folder) throws Exception {
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry.isDirectory()) {
                    continue;
                } else {
                    readFile(fileEntry);
                }
            }
        }

        public static void readFile(File file) throws Exception {

            BufferedReader br = new BufferedReader(new FileReader(file));

            String str;
            while ((str = br.readLine()) != null){
                System.out.println(str);
                String c0[] = str.split("\t");
                if (c0.length < 2) {
                    Constants.printDebug("MapSideJoin - setupOrderHashMap input is wrong c0 is:" + str);
                }
                C0_data.put(c0[0],c0[1]);
            }
        }

        public static void getC0_From_S3() throws Exception {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    TransferManager transferManager = new TransferManager(new DefaultAWSCredentialsProviderChain());
                    File dir = new File(Constants.LOCAL_WORD_COUNT_C0_OUTPUT);

                    MultipleFileDownload download =  transferManager.downloadDirectory(Constants.OUTPUT_BUCKET_NAME , Constants.WORD_COUNT_C0_OUTPUT, dir);
                    try {
                        download.waitForCompletion();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "jc");
            t.setDaemon(true);
            t.start();
            t.join();

            final File folder = new File(Constants.LOCAL_WORD_COUNT_C0_OUTPUT + "/" +Constants.WORD_COUNT_C0_OUTPUT);
            listFilesForFolder(folder);
            System.out.println(C0_data);
        }

        @Override
        protected void setup(Context context) {
            try {
                getC0_From_S3();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] splitted = value.toString().split("\t");
            if (splitted.length < 1) {
                Constants.printDebug("MapSideJoin - input is not as expected, value is: " + value);
                return;
            }

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


    public static Job createJoinJob() {
        Job job_join_C0=null;

        try {
            // The reducer join output file will be from the following format ((w1,w2,w3) N3 N1 N2 C1 C2)

            job_join_C0 = CreateJoinJob(
                    Constants.JOB_JOIN_C0,
                    Constants.getS3OutputPath(Constants.JOIN_OUTPUT4),
                    Constants.getS3OutputPath(Constants.JOIN_OUTPUT3));
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Constants.printDebug("finished creating map side join job");
        return  job_join_C0;
    }


}

