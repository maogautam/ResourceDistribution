import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by maogautam on 1/8/16.
 */
public class ResourceDistributionMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

        String district= value.toString().split(",")[7];
        context.write(new Text(district),new IntWritable(1));
    }
}
