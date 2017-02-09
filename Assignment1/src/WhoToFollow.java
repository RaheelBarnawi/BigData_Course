
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.util.function.Predicate;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class WhoToFollow extends Configured implements Tool

{
	public final Log log = LogFactory.getLog(Mapper.class);
	private static final String OUTPUT_PATH = "intermediate_output";

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Job 1
		 */
		Configuration conf = new Configuration();//getConf();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "Job1");
		job.setJarByClass(WhoToFollow.class);
		job.setMapperClass(FirstMap.class);
		job.setReducerClass(FirstReduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		job.waitForCompletion(true);

		/*
		 * Job 2
		 */

		Configuration conf1 = new Configuration();//getConf();
		Job job2 = Job.getInstance(conf1, "Job 2");
		job2.setJarByClass(WhoToFollow.class);
		job2.setMapperClass(SecondMap.class);
		job2.setReducerClass(SecondReduce.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true) ? 0 : 1;
	}



	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(0);
		}
		ToolRunner.run(new Configuration(), new WhoToFollow(), args);
	}
}

