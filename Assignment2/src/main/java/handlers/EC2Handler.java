package handlers;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;



public class EC2Handler {

    private AWSCredentialsProvider credentials;
    private AmazonEC2 ec2;

    /**
     * For a client - create our credentials file at ~/.aws/credentials
     * For non client - gets the credentials from the role used to create this instance
     * Initialize a connection with our EC2
     */
    public EC2Handler() {
        createCredentials();
        this.ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    private void createCredentials() {
        this.credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
    }

    public AWSCredentialsProvider getCredentials() {
        return credentials;
    }


    public AmazonEC2 getEc2() {
        return ec2;
    }


    /**
     * List all ec2 instances with their status and tags
     */
    public List<Instance> listInstances(boolean print) {
        List<Instance> instances = new LinkedList<>();
        boolean done = false;   // done = True - when finished going over all the instances.
        DescribeInstancesRequest instRequest = new DescribeInstancesRequest();

        try {
            while (!done) {

                // Go through all instances
                DescribeInstancesResult response = this.ec2.describeInstances(instRequest);

                for (Reservation reservation : response.getReservations()) {
                    for (Instance instance : reservation.getInstances()) {

                        String state = instance.getState().getName();

                        StringBuilder tagsBuilder = new StringBuilder();
                        tagsBuilder.append("tags: ");
                        Filter filter = new Filter().withName("resource-id").withValues(instance.getInstanceId());
                        DescribeTagsRequest tagRequest = new DescribeTagsRequest().withFilters(filter);
                        DescribeTagsResult tagResult = this.ec2.describeTags(tagRequest);
                        List<TagDescription> tags = tagResult.getTags();
                        for (TagDescription tagDesc : tags) {
                            tagsBuilder.append(tagDesc.getValue());
                            tagsBuilder.append(" ");
                        }

                        if (print)
                            System.out.println("instance: " + instance.getInstanceId() + ", state: " + state + ", with tags: " + tagsBuilder.toString());
                        instances.add(instance);
                    }
                }

                instRequest.setNextToken(response.getNextToken());
                if (response.getNextToken() == null) {
                    done = true;
                }
            }
            return instances;
        } catch (AmazonServiceException ase) {
            printASEException(ase);
            return null;
        }
    }

    /**
     * prints AmazonServiceException description
     *
     * @param ase - AmazonServiceException
     */
    private void printASEException(AmazonServiceException ase) {
        System.out.println("Caught Exception: " + ase.getMessage());
        System.out.println("Response Status Code: " + ase.getStatusCode());
        System.out.println("Error Code: " + ase.getErrorCode());
        System.out.println("Request ID: " + ase.getRequestId());

    }

    public String getStat() {

        AmazonCloudWatch cloudWatchClient = AmazonCloudWatchClientBuilder
                .standard()
                .withCredentials(credentials)
                .build();

        StringBuilder statistics = new StringBuilder();

        boolean done = false;   // done = True - when finished going over all the instances.
        DescribeInstancesRequest instRequest = new DescribeInstancesRequest();

        try {
            while (!done) {

                // Go through all instances
                DescribeInstancesResult response = this.ec2.describeInstances(instRequest);

                for (Reservation reservation : response.getReservations()) {
                    for (Instance instance : reservation.getInstances()) {

                        statistics.append("\n\n********************************************");
                        statistics.append("\n\nInstance id: " + instance.getInstanceId());
                        statistics.append("\ntag: " + instance.getTags().toString());

                        long offsetInMilliseconds = 1000 * 60 * 60;
                        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
                                .withStartTime(new Date(new Date().getTime() - offsetInMilliseconds))
                                .withNamespace("AWS/EC2")
                                .withPeriod(60 * 30)
                                .withDimensions(new Dimension()
                                        .withName("InstanceId")
                                        .withValue(instance.getInstanceId()))
                                .withMetricName("CPUUtilization")
                                .withStatistics("Average", "Maximum")
                                .withEndTime(new Date());
                        GetMetricStatisticsResult getMetricStatisticsResult = cloudWatchClient
                                .getMetricStatistics(request);

                        List dataPoint = getMetricStatisticsResult.getDatapoints();
                        for (Object aDataPoint : dataPoint) {
                            Datapoint dp = (Datapoint) aDataPoint;
                            statistics.append("\n   Timestamp: " + dp.getTimestamp());
                            statistics.append("\n   Average: " + dp.getAverage());
                            statistics.append("\n   Maximum: " + dp.getMaximum());
                            statistics.append("\n   Minimum: " + dp.getMinimum());
                            statistics.append("\n   Unit: " + dp.getUnit());
                        }
                        statistics.append("\n\n********************************************");
                    }
                }

                instRequest.setNextToken(response.getNextToken());
                if (response.getNextToken() == null) {
                    done = true;
                }
            }

            return statistics.toString();

        } catch (AmazonServiceException ase) {
            printASEException(ase);
            return "";
        }
    }
}



