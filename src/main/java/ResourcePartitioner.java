import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by maogautam on 1/8/16.
 */
public class ResourcePartitioner extends Partitioner<Text,IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        if(i==0)
            return 0;

        else if(Integer.parseInt(text.toString())<=85)
        {
            return  0;
        }
        else if(Integer.parseInt(text.toString())>=85||Integer.parseInt(text.toString())<=170 )
        {
            return 1;
        }
        else
            return 2;
    }
}
