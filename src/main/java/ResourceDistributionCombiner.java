import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by maogautam on 1/8/16.
 */
public class ResourceDistributionCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {



    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int districtReportCount = 0;

        while (values.iterator().hasNext()) {
            districtReportCount++;
        }

            context.write(key, new IntWritable(districtReportCount));

    }
}
