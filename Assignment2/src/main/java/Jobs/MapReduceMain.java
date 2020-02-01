package Jobs;

import com.google.inject.internal.cglib.core.$Constants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.util.LinkedList;

public class MapReduceMain {


    public static void main(String[] args) {

        try {
            String oneGramLink = args[0];
            String twoGramLink = args[1];
            String threeGramLink = args[2];
            String outputLink = args[3];

            Constants.printDebug("1gram: " + oneGramLink);
            Constants.printDebug("2gram: " + twoGramLink);
            Constants.printDebug("3gram: " + threeGramLink);
            Constants.printDebug("output: " + outputLink);

            // WORKS FROM HERE
            JobControl jobControl = createJobs(oneGramLink, twoGramLink, threeGramLink);
            Thread t = new Thread(jobControl, "jc");
            t.setDaemon(true);
            t.start();
            while (!jobControl.allFinished()) {
                if (!jobControl.getFailedJobList().isEmpty()) {
                    throw new RuntimeException("at least 1 job failed: " + jobControl.getFailedJobList().toString());
                }
                Constants.printDebug("Waiting");
                Thread.sleep(2000);
            }

            Constants.printDebug("succesful jobs so far: " + jobControl.getSuccessfulJobList().toString());
            Constants.printDebug("failed jobs so far: " + jobControl.getFailedJobList().toString());
            Constants.printDebug("waiting jobs so far: " + jobControl.getWaitingJobList().toString());
            Constants.printDebug("");

            System.exit(0);

        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }


    private static JobControl createJobs(String oneGram, String twoGram, String threeGram) throws IOException {
        Job[] occJobs = NGramsOcc.createOccJobs(oneGram, twoGram, threeGram);
        Job wordCounterJob = WordCounter.createWordCountJob();
        Job[] joinJobs_N3_N1_N2_C1_C2 = ReduceSideJoin.createJoinJobs();
        Job joinJob_C0 = MapSideJoin.createJoinJob();
        Job probabilityWithSort = ProbabilityWithSort.createProbabilityWithSortJob();

        ControlledJob occ_1grams = new ControlledJob(occJobs[0], new LinkedList<>()); // N1
        ControlledJob occ_2grams = new ControlledJob(occJobs[1], new LinkedList<>()); //N2
        ControlledJob occ_3grams = new ControlledJob(occJobs[2], new LinkedList<>()); //N3
        ControlledJob wordCounter = new ControlledJob(wordCounterJob, new LinkedList<>()); //C0

        ControlledJob join_N1 = new ControlledJob(joinJobs_N3_N1_N2_C1_C2[0], new LinkedList<>());
        ControlledJob join_N2 = new ControlledJob(joinJobs_N3_N1_N2_C1_C2[1], new LinkedList<>());
        ControlledJob join_C1 = new ControlledJob(joinJobs_N3_N1_N2_C1_C2[2], new LinkedList<>());
        ControlledJob join_C2 = new ControlledJob(joinJobs_N3_N1_N2_C1_C2[3], new LinkedList<>());
        ControlledJob join_C0 = new ControlledJob(joinJob_C0, new LinkedList<>());
        ControlledJob prob_sort = new ControlledJob(probabilityWithSort, new LinkedList<>());


        //dependencies for stage 2
        wordCounter.addDependingJob(occ_1grams);

        join_N1.addDependingJob(occ_1grams);
        join_N1.addDependingJob(occ_3grams);

        join_N2.addDependingJob(join_N1);
        join_C1.addDependingJob(join_N2);
        join_C2.addDependingJob(join_C1);
        join_C0.addDependingJob(join_C2);
        prob_sort.addDependingJob(join_C0);

        JobControl jobControl = new JobControl("JC");

        jobControl.addJob(occ_1grams);
        jobControl.addJob(occ_2grams);
        jobControl.addJob(occ_3grams);
        jobControl.addJob(wordCounter);

        jobControl.addJob(join_N1);
        jobControl.addJob(join_N2);
        jobControl.addJob(join_C1);
        jobControl.addJob(join_C2);
        jobControl.addJob(join_C0);
        jobControl.addJob(prob_sort);
        return jobControl;
    }
}
