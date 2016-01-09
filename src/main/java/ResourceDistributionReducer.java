import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by maogautam on 1/8/16.
 */
public class ResourceDistributionReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

     int totalReportCount= 3500*365*2;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int districtReportCount=0;

        while(values.iterator().hasNext()){
          districtReportCount ++;
        }
       int ratio= (int) ((int)(districtReportCount/totalReportCount) + 0.5f);

        if(ratio<=1)
        {
            context.write(key,new IntWritable(1));
        }
        else
        {
            int diff= ratio-1;
            int alloc= 350 * (diff/100);
            context.write(key,new IntWritable(1+alloc));
        }



    }
}
