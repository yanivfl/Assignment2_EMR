package InputFormats;

import Jobs.Constants;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class ProbabilityKey implements Writable, WritableComparable<ProbabilityKey> {

    private DoubleWritable probability;
    private Text w1_w2;
    private Text w1_w2_w3;

    public ProbabilityKey(String w1_w2_w3, double probability) {
        String[] word = w1_w2_w3.split(" ");
        if (word.length < 2)
            Constants.printDebug("");   // TODO

        this.w1_w2 = new Text(word[0] + " " + word[1]);
        this.w1_w2_w3 = new Text(w1_w2_w3);
        this.probability = new DoubleWritable(probability);
    }

    public ProbabilityKey() {
        this.w1_w2 = new Text();
        this.w1_w2_w3 = new Text();
        this.probability = new DoubleWritable(0);
    }

    @Override
    public void readFields(DataInput data) throws IOException {
        probability.readFields(data);
        w1_w2.readFields(data);
        w1_w2_w3.readFields(data);
    }

    @Override
    public void write(DataOutput data) throws IOException {
        probability.write(data);
        w1_w2.write(data);
        w1_w2_w3.write(data);
    }

    @Override
    public String toString() {
        return "ProbabilityKey{" +
                "probability=" + probability +
                ", w1_w2=" + w1_w2 +
                ", w1_w2_w3=" + w1_w2_w3 +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProbabilityKey that = (ProbabilityKey) o;
        return Objects.equals(probability, that.probability) &&
                Objects.equals(w1_w2, that.w1_w2) &&
                Objects.equals(w1_w2_w3, that.w1_w2_w3);
    }

    @Override
    public int compareTo(ProbabilityKey o) {

        // compare by w1_w2, if they are equal, compare by the probability of (w3 | w1_w2)
        int comp = this.w1_w2.toString().compareTo(o.getW1_w2().toString());

        if (comp == 0)
            return (-1) * this.probability.toString().compareTo(o.getProbability().toString());

        return comp;

    }

    public DoubleWritable getProbability() {
        return probability;
    }

    public Text getW1_w2() {
        return w1_w2;
    }

    public Text getW1_w2_w3() {
        return w1_w2_w3;
    }

    public void setProbability(DoubleWritable probability) {
        this.probability = probability;
    }

    public void setW1_w2(Text w1_w2) {
        this.w1_w2 = w1_w2;
    }

    public void setW1_w2_w3(Text w1_w2_w3) {
        this.w1_w2_w3 = w1_w2_w3;
    }
}