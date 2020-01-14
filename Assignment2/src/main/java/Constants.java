import org.apache.commons.io.FileUtils;

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

//    public static final String N2 = "N2";
//    public static final String N3 = "N3";
//    public static final String C0 = "C0";
//    public static final String C1 = "C1";
//    public static final String C2 = "C2";

    // output directories names
    public static final String OCC_1_GRAMS = "mini_counter_1_grams";
    public static final String OCC_2_GRAMS = "mini_counter_2_grams";
    public static final String OCC_3_GRAMS = "mini_counter_3_grams";
    public static final String WORD_COUNT_C0 = "word_count_C0";

    //// output directories names - s3 TODO


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
