package Jobs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;




public class Main {

    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider.getCredentials());

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(Constants.getS3Path(Constants.INPUT_BUCKET_NAME, Constants.MY_JAR_NAME)) // This should be a full map reduce application. TODO
//                    .withMainClass("some.pack.MainClass")
                .withArgs(Constants.getS3NgramLink(1),
                        Constants.getS3NgramLink(2),
                        Constants.getS3NgramLink(3),
                        Constants.getS3Path(Constants.OUTPUT_BUCKET_NAME, Constants.OUTPUT_FILE_NAME)
                );

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(4)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName(Constants.MY_KEY)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)

                // TODO: getEMR roles
                .withLogUri(Constants.getS3Path(Constants.OUTPUT_BUCKET_NAME, "logs/"));

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
