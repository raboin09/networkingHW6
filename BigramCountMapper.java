
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.StringBuilder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BigramCountMapper extends Mapper<LongWritable, Text, BigramWritable, Text> {

    private static final IntWritable ONE = new IntWritable(1);
    private static final BigramWritable BIGRAM = new BigramWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        String[] arr;
        int countOfTokens = 0;
        while (itr.hasMoreTokens()) {
            countOfTokens++;
            itr.nextToken();
        }

        itr = new StringTokenizer(line);
        arr = new String[countOfTokens];
        
        for (int i = 0; i < countOfTokens; i++) {
            arr[i] = itr.nextToken();
        }

        String currentPerson = arr[0];
        for (int i = 1; i < arr.length; i++) {
            String cur = arr[i];
            StringBuilder comFriends = new StringBuilder();
            if (Integer.parseInt(currentPerson) < Integer.parseInt(cur)) {
                BIGRAM.set(new Text(currentPerson), new Text(cur));
            } else if (Integer.parseInt(currentPerson) > Integer.parseInt(cur)) {
                BIGRAM.set(new Text(cur), new Text(currentPerson));
            }
            
            for (int j = 1; j < arr.length; j++) {
                if (j != i ) {
                    comFriends.append(arr[j]);
                    if(j<arr.length-1){
                        comFriends.append(",");
                    }
                }
            }
            
            if(comFriends.charAt(comFriends.length()-1)==','){
                comFriends.deleteCharAt(comFriends.length()-1);
            }
            
            String valueAdd = comFriends.toString();
            context.write(BIGRAM, new Text(valueAdd));
        }
    }
}
