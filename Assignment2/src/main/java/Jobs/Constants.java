package Jobs;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.io.Text;
import java.io.File;
import java.io.IOException;

public class Constants {


    // mini-examples corpus ngrams file names
    public static final String CORPUS_1_GRAMS = "mini_corpus_1_grams.txt";
    public static final String CORPUS_2_GRAMS = "mini_corpus_2_grams.txt";
    public static final String CORPUS_3_GRAMS = "mini_corpus_3_grams.txt";


    // corpus ngrams S3 paths
    // TODO

    // job names
    public static final String JOB_1_GRAM = "JOB_1_GRAM";
    public static final String JOB_2_GRAM = "JOB_2_GRAM";
    public static final String JOB_3_GRAM = "JOB_3_GRAM";
    public static final String JOB_C0 = "JOB_C0";
    public static final String JOB_JOIN_N1 = "JOB_JOIN_N1";
    public static final String JOB_JOIN_N2 = "JOB_JOIN_N2";
    public static final String JOB_JOIN_C1 = "JOB_JOIN_C1";
    public static final String JOB_JOIN_C2 = "JOB_JOIN_C2";


//    public static final String N2 = "N2";
//    public static final String N3 = "N3";
//    public static final String C0 = "C0";
//    public static final String C1 = "C1";
//    public static final String C2 = "C2";

    // output directories names
    public static final String OCC_1_GRAMS_OUTPUT = "mini_counter_1_grams";
    public static final String OCC_2_GRAMS_OUTPUT = "mini_counter_2_grams";
    public static final String OCC_3_GRAMS_OUTPUT = "mini_counter_3_grams";
    public static final String WORD_COUNT_C0_OUTPUT = "word_count_C0";
    public static final String JOIN_OUTPUT = "join";
    public static final String JOIN_OUTPUT1 = "join1";
    public static final String JOIN_OUTPUT2 = "join2";
    public static final String JOIN_OUTPUT3 = "join3";

    //// output directories names - s3 TODO


    // tags
    public static final Text TAG_1_OCC = new Text("1");
    public static final Text TAG_2_OCC = new Text("2");
    public static final Text TAG_3_OCC = new Text("3");


    /* If there is an output folder, delete it */
    public static void clearOutput(String path) {
        File file = new File(path);

        if (file.exists())  {
            try {
                FileUtils.deleteDirectory(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    final static boolean isDebug = true;

    public static void printDebug(String str) {
        if (isDebug) {
            System.out.println(str);
        }
    }
}
