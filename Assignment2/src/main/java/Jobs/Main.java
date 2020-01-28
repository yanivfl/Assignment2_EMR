package Jobs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import sun.awt.geom.AreaOp;
import sun.misc.JavaOISAccess;

import java.io.IOException;
import java.util.LinkedList;

public class Main {


    public static void main(String[] args) {

//        Job[] occJobs = NGramsOcc.createOccTables();
//        Job wordCounterJob = WordCounter.createWordCountTable();
        Job[] joinJobs = ReduceSideJoin.createJoinTable();

        try {
//            ControlledJob occ_1grams = new ControlledJob(occJobs[0], new LinkedList<>()); // N1
//            ControlledJob occ_2grams = new ControlledJob(occJobs[1], new LinkedList<>()); //N2
//            ControlledJob occ_3grams = new ControlledJob(occJobs[2], new LinkedList<>()); //N3
//            ControlledJob wordCounter = new ControlledJob(wordCounterJob, new LinkedList<>()); //C0


            ControlledJob join_N1 = new ControlledJob(joinJobs[0], new LinkedList<>());


            //dependencies for stage 2
//            wordCounter.addDependingJob(occ_1grams);
//
//            join_N1.addDependingJob(occ_1grams);
//            join_N1.addDependingJob(occ_3grams);


            JobControl jobControl = new JobControl("JC");

//            jobControl.addJob(occ_1grams);
//            jobControl.addJob(occ_2grams);
//            jobControl.addJob(occ_3grams);
//            jobControl.addJob(wordCounter);

            jobControl.addJob(join_N1);







            Thread t = new Thread(jobControl, "jc");
            t.setDaemon(true);
            t.start();
            while (!jobControl.allFinished()) {
                if (!jobControl.getFailedJobList().isEmpty()) {
                    throw new RuntimeException("at least 1 job failed: " + jobControl.getFailedJobList().toString());
                }
                Constants.printDebug("waiting jobs: " + jobControl.getReadyJobsList().toString());
                Constants.printDebug("succesful jobs so far: " + jobControl.getSuccessfulJobList().toString());
                Constants.printDebug("Waiting");
                Thread.sleep(2000);
            }

            Constants.printDebug("succesful jobs so far: " + jobControl.getSuccessfulJobList().toString());
            Constants.printDebug("failed jobs so far: " + jobControl.getFailedJobList().toString());
            Constants.printDebug("waitng jobs so far: " + jobControl.getWaitingJobList().toString());
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
// catch (InterruptedException e) {
//            e.printStackTrace();
//        }


    }
}
