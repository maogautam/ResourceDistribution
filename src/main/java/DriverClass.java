import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * Created by maogautam on 1/8/16.
 */
public class DriverClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2)
        {
            System.exit(0);
        }

//        Configuration conf =  new  Configuration();
//        Job job = Job.getInstance(conf, "Patent Citation Example");
        Job job =new Job();
        job.setJarByClass(DriverClass.class );
        //set number of reducer
        job.setNumReduceTasks(3);
        //setting map reduce class
        job.setMapperClass(ResourceDistributionMapper.class);
        job.setReducerClass(ResourceDistributionReducer.class);
        //set combiner and partitioner
        job.setCombinerClass(ResourceDistributionReducer.class);
        job.setPartitionerClass(ResourcePartitioner.class);
        //Set Input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 :  1 );
    }
}
