import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BigramCountReducer extends Reducer<BigramWritable, Text, BigramWritable, Text> {
    
    private final static IntWritable SUM = new IntWritable();
    
    @Override
    public void reduce(BigramWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String intersection = "";
        for(Text line : values){
            intersection += line + " ";
        }
        context.write(key, new Text(intersection));
        
    }
    
}
