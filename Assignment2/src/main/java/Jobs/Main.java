package Jobs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.util.LinkedList;

public class Main {


    public static void main(String[] args) {

        Job[] occJobs = NGramsOcc.createOccTables();
        Job wordCounterJob = WordCounter.createWordCountTable();

        try {
            ControlledJob occ_1grams = new ControlledJob(occJobs[0], new LinkedList<ControlledJob>());
            ControlledJob occ_2grams = new ControlledJob(occJobs[1], new LinkedList<ControlledJob>());
            ControlledJob occ_3grams = new ControlledJob(occJobs[2], new LinkedList<ControlledJob>());
            ControlledJob wordCounter = new ControlledJob(wordCounterJob, new LinkedList<ControlledJob>());

            wordCounter.addDependingJob(occ_1grams);

            JobControl jobControl = new JobControl("JC");

            jobControl.addJob(occ_1grams);
            jobControl.addJob(occ_2grams);
            jobControl.addJob(occ_3grams);
            jobControl.addJob(wordCounter);

            Thread t = new Thread(jobControl, "jc");
            t.setDaemon(true);
            t.start();
            while (!jobControl.allFinished()) {
                Constants.printDebug("Waiting");
                Thread.sleep(2000);
            }

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
