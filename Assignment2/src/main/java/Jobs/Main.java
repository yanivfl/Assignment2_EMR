package Jobs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.inject.internal.cglib.core.$Constants;
import handlers.S3Handler;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Main {

    public static void main(String[] args) throws IOException {
        AmazonS3 s3 = new S3Handler(true).getS3();
        S3Object s3object = s3.getObject(new GetObjectRequest(
                Constants.OUTPUT_BUCKET_NAME, Constants.WORD_COUNT_C0_OUTPUT + "/part-r-00006"));
        System.out.println(s3object.getObjectMetadata().getContentType());
        System.out.println(s3object.getObjectMetadata().getContentLength());

        BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
        String line;
        while((line = reader.readLine()) != null) {
            // can copy the content locally as well
            // using a buffered writer
            System.out.println(line);
        }



    }
//        String inDirname = "s3://"+Constants.OUTPUT_BUCKET_NAME+"/" + Constants.WORD_COUNT_C0_OUTPUT;
//        String outFilename = "co_output.txt";
//
//        Configuration conf = new Configuration();
//        FileSystem hdfs = FileSystem.get(URI.create(inDirname), conf);
//        FileSystem local = FileSystem.getLocal(conf);
//
//        Path inputPath =new Path(inDirname);
//        inputPath = inputPath.getFileSystem(conf).makeQualified(inputPath);
//
//        Path outFile = new Path(outFilename);
//        OutputStream outstream = local.create(outFile);
//        PrintWriter writer = new PrintWriter(outstream);
////        Path inDir = new Path(inDirname);
//
//        for (FileStatus inFile : hdfs.listStatus(inputPath)) {
//            InputStream instream = hdfs.open(inFile.getPath());
//
//            BufferedReader reader = new BufferedReader(
//                    new InputStreamReader(instream));
//            String line=null;
//            while ((line = reader.readLine()) != null)
//                writer.println(line);
//            reader.close();
//        }
//        writer.close();
//    }



//        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
//                .withRegion("us-east-1")
//                .build();
//
//        Constants.printDebug("hadoop jar step");
//        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
//                .withJar(Constants.getS3Path(Constants.INPUT_BUCKET_NAME, Constants.MY_JAR_NAME))
////                    .withMainClass("some.pack.MainClass")
//                .withArgs(
//                        Constants.getS3NgramLink(1),
//                        Constants.getS3NgramLink(2),
//                        Constants.getS3NgramLink(3),
//                        Constants.getS3OutputPath(Constants.OUTPUT_FILE_NAME)
//                );
//
//        Constants.printDebug("stepConfig jar step");
//
//        StepConfig stepConfig = new StepConfig()
//                .withName("stepname")
//                .withHadoopJarStep(hadoopJarStep)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");
//
//        Constants.printDebug("jobFlowConfig jar step");
//
//        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
//                .withInstanceCount(6)
//                .withMasterInstanceType(InstanceType.M1Large.toString())
//                .withSlaveInstanceType(InstanceType.M1Large.toString())
//                .withHadoopVersion("2.6.0").withEc2KeyName(Constants.MY_KEY)
//                .withKeepJobFlowAliveWhenNoSteps(false)
//                .withPlacement(new PlacementType("us-east-1a"));
//
//        Constants.printDebug("RunJobFlow jar step");
//
//        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
//                .withName("jobname")
//                .withInstances(instances)
//                .withReleaseLabel("emr-5.1.0")
//                .withSteps(stepConfig)
//                .withJobFlowRole("EMR_EC2_DefaultRole")
//                .withServiceRole("EMR_DefaultRole")
//                .withLogUri(Constants.getS3OutputPath("logs/"));
//
//        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
//        String jobFlowId = runJobFlowResult.getJobFlowId();
//        System.out.println("Ran job flow with id: " + jobFlowId);

//    }
}
