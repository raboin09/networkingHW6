
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.StringBuilder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BigramCountMapper extends Mapper<LongWritable, Text, BigramWritable, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private static final BigramWritable BIGRAM = new BigramWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String prev = null;
        StringTokenizer itr = new StringTokenizer(line);
        String[] arr = new String[itr.countTokens()];
        for(int i=0; i<itr.countTokens(); i++){
            arr[i] = itr.nextToken().toString();
        }
        BIGRAM.set(new Text("arr[0]"), new Text(arr[0]));
        context.write(BIGRAM, ONE);
        String currentPerson = arr[0];
        for (int i = 0; i < arr.length; i++) {
            String cur = arr[i];
            StringBuilder comFriends = new StringBuilder();
            comFriends.append("{");
            if (prev != null && cur != null) {
                if (Integer.parseInt(currentPerson) < Integer.parseInt(cur)) {
                    for(int j=0; j<arr.length; j++){
                        if(j!= i && j != Integer.parseInt(currentPerson)){
                            comFriends.append(arr[j]); 
                        }
                    }
                    comFriends.append("}");
                    String valueAdd = comFriends.toString();
                    BIGRAM.set(new Text("(" + currentPerson + "," + cur + ")"), new Text(valueAdd));
                } else if (Integer.parseInt(currentPerson) > Integer.parseInt(cur)) {
                    for(int j=0; j<arr.length; j++){
                        if(j!= i && j != Integer.parseInt(currentPerson)){
                            comFriends.append(arr[j]); 
                        }
                    }
                    comFriends.append("}");
                    String valueAdd = comFriends.toString();
                    BIGRAM.set(new Text("(" + cur + "," + currentPerson + ")"), new Text(valueAdd));
                }
                context.write(BIGRAM, ONE);
            }
            prev = cur;
        }

        /*while (itr.hasMoreTokens()) {
            String cur = itr.nextToken();
            if (prev != null) {
                BIGRAM.set(new Text(prev), new Text(cur));
                context.write(BIGRAM, ONE);
            }
            prev = cur;
        }*/
    }
}
