package Jobs;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.io.Text;
import java.io.File;
import java.io.IOException;

public class Constants {

    public static final String CORPUS_1_GRAMS = "mini_corpus_new_1_grams.txt";
    public static final String CORPUS_2_GRAMS = "mini_corpus_new_2_grams.txt";
    public static final String CORPUS_3_GRAMS = "mini_corpus_new_3_grams.txt";

    // job names
    public static final String JOB_1_GRAM = "JOB_1_GRAM";
    public static final String JOB_2_GRAM = "JOB_2_GRAM";
    public static final String JOB_3_GRAM = "JOB_3_GRAM";
    public static final String JOB_C0 = "JOB_C0";
    public static final String JOB_JOIN_N1 = "JOB_JOIN_N1";
    public static final String JOB_JOIN_N2 = "JOB_JOIN_N2";
    public static final String JOB_JOIN_C1 = "JOB_JOIN_C1";
    public static final String JOB_JOIN_C2 = "JOB_JOIN_C2";
    public static final String JOB_JOIN_C0 = "JOB_JOIN_C0";
    public static final String JOB_PROB_WITH_SORT = "JOB_PROB_WITH_SORT";

    // output directories names
    public static final String OCC_1_GRAMS_OUTPUT = "mini_counter_1_grams";
    public static final String OCC_2_GRAMS_OUTPUT = "mini_counter_2_grams";
    public static final String OCC_3_GRAMS_OUTPUT = "mini_counter_3_grams";
    public static final String WORD_COUNT_C0_OUTPUT = "word_count_C0";
    public static final String JOIN_OUTPUT = "join";
    public static final String JOIN_OUTPUT1 = "join1";
    public static final String JOIN_OUTPUT2 = "join2";
    public static final String JOIN_OUTPUT3 = "join3";
    public static final String JOIN_OUTPUT4 = "join4";
    public static final String JOIN_OUTPUT5 = "join5";


    public static String getS3NgramLink(int ngram){
        return "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/"+ngram+"gram/data";
    }


    public static final String MY_KEY = "my_key";
    public static final String OUTPUT_FILE_NAME = "output";
    public static final String OUTPUT_BUCKET_NAME = "dsps-201-assignment2-yaniv-yuval-output";
    public static final String INPUT_BUCKET_NAME = "dsps-201-assignment2-yaniv-yuval-input";
    public static final String MY_JAR_NAME = "Assignment2.jar";
    public static final String S3 = "s3://";

    public static String getS3Path(String bucketName, String fileName) {
        return S3 + bucketName + "/" + fileName;
    }

    public static String getS3OutputPath(String fileName) {
        return S3 + OUTPUT_BUCKET_NAME + "/" + fileName;
    }


    // tags
    public static final Text TAG_1_OCC = new Text("1");
    public static final Text TAG_2_OCC = new Text("2");
    public static final Text TAG_3_OCC = new Text("3");


    public static final int W1_W2_W3_IDX = 0;
    public static final int N3_IDX = 1;
    public static final int N1_IDX = 2;
    public static final int N2_IDX = 3;
    public static final int C1_IDX = 4;
    public static final int C2_IDX = 5;
    public static final int C0_IDX = 6;


    /* If there is an output folder, delete it */
    public static void clearOutput(String path) {
//        File file = new File(path);
//
//        if (file.exists())  {
//            try {
//                FileUtils.deleteDirectory(file);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }


    final static boolean isDebug = true;

    public static void printDebug(String str) {
        if (isDebug) {
            System.out.println(str);
        }
    }
}
