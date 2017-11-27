import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BigramWritable implements WritableComparable<BigramWritable> {

    private Text leftBigram;
    private Text rightBigram;

    //default constructor
    public BigramWritable() {
    }

    //constructor, Text inputs
    public BigramWritable(Text left, Text right) {
        this.leftBigram = left;
        this.rightBigram = right;
    }

    //convert DataInput from bits to Text
    public void readFields(DataInput in) throws IOException {
        leftBigram = new Text(in.readUTF());
        rightBigram = new Text(in.readUTF());
    }

    //convert Text to bits
    public void write(DataOutput out) throws IOException {
        out.writeUTF(leftBigram.toString());
        out.writeUTF(rightBigram.toString());
    }

    //set method
    public void set(Text prev, Text cur) {
        leftBigram = prev;
        rightBigram = cur;
    }

    @Override
    public String toString() {
        return leftBigram.toString() + " " + rightBigram.toString();
    }

    //return sum of left and right
    @Override
    public int hashCode() {
        return leftBigram.hashCode() + rightBigram.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BigramWritable) {
            BigramWritable bigram = (BigramWritable) o;
            return leftBigram.equals(bigram.leftBigram)
                    && rightBigram.equals(bigram.rightBigram);
        }
        return false;
    }

    //check for bigram equality 
    //Ex.: (1, 0) (1, 0) return 0
    public int compareTo(BigramWritable tp) {
        int cmp = leftBigram.compareTo(tp.leftBigram);
        if (cmp != 0) {
            return cmp;
        }
        return rightBigram.compareTo(tp.rightBigram);
    }

}

