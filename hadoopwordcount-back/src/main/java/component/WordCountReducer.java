package component;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * function:the reduce
 *
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int temp=0;
        for(IntWritable count:values){
            temp=temp+count.get();
        }
        context.write(new Text(key),new IntWritable(temp));
    }
}
