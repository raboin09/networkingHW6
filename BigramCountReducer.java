
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.lang.StringBuilder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BigramCountReducer extends Reducer<BigramWritable, Text, BigramWritable, Text> {

    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(BigramWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        
        String str1 = "";
        String str2 = "";
        StringBuilder commonFriends = new StringBuilder();
        commonFriends.append("Common Friends: ");
        boolean first = true;

        for(Text val : values){
            if(first==true){
                str1=val.toString();
                first = false;
            }
            else{
                str2=val.toString();
            }
        }
        
        context.write(key, new Text(str1));
        context.write(key, new Text(str2));
        
        String[] strArr1 = str1.split("\\,");
        String[] strArr2 = str2.split("\\,");

        for (int i = 0; i < strArr1.length; i++) {
            for (int j = 0; j < strArr2.length; j++) {
                if(strArr1[i]!="" && strArr2[j]!=""){
                    if (Integer.parseInt(strArr1[i]) != Integer.parseInt(strArr2[j])) {
                        commonFriends.append(strArr1[i]);
                        commonFriends.append(", ");
                    }
                }
            }
        }

        String out = commonFriends.toString();
        context.write(key, new Text(out));

    }

}
