
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
public class WhoToFollow extends Configured implements Tool

{

	private static final String OUTPUT_PATH = "intermediate_output";

	 @Override
	 public int run(String[] args) throws Exception {
	  /*
	   * Job 1
	   */
	  Configuration conf = getConf();
	 // FileSystem fs = FileSystem.get(conf);
	  Job job = new Job(conf, "Job1");
	  job.setJarByClass(WhoToFollow.class);

	  job.setMapperClass(Mapper.class);
	  job.setReducerClass(Reducer.class);

	  job.setOutputKeyClass(IntWritable.class);
	  job.setOutputValueClass(IntWritable.class);
	  //job.setMapOutputKeyClass(IntWritable.class);
	 // job.setMapOutputValueClass(IntWritable.class);

	 
	  job.setOutputFormatClass(TextOutputFormat.class);
	 job.setInputFormatClass(KeyValueTextInputFormat.class);

	 FileInputFormat.addInputPath(job, new Path(args[0]));
	  TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	  job.waitForCompletion(true);

	  /*
	   * Job 2
	   */
	  
	  Job job2 = new Job(conf, "Job 2");
	  job2.setJarByClass(WhoToFollow.class);

	  job2.setMapperClass(MapRecommendation.class);
	  job2.setReducerClass(ReduceRecommendation.class);

	  //reducer output(k,v) classes 
	  job2.setOutputKeyClass(IntWritable.class);
	  job2.setOutputValueClass(IntWritable.class);
	  
	  //job2.setMapOutputKeyClass(IntWritable.class);
	  //job2.setMapOutputValueClass(IntWritable.class);


	 job2.setInputFormatClass(SequenceFileInputFormat.class);
	 job2.setOutputFormatClass(TextOutputFormat.class);

	  FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	  TextOutputFormat.setOutputPath(job2, new Path(args[1]));

	  return job2.waitForCompletion(true) ? 0 : 1;
	 }

	 /**
	  * Method Name: main Return type: none Purpose:Read the arguments from
	  * command line and run the Job till completion
	  * 
	  */
	 public static void main(String[] args) throws Exception {
	  // TODO Auto-generated method stub
	  if (args.length != 2) {
	   System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
	   System.exit(0);
	  }
	  System.out.println("Hi there ");
	  ToolRunner.run(new Configuration(), new WhoToFollow(), args);
	 }

	

	public static class Map extends Mapper<Text, Text, IntWritable, IntWritable> {
		public final Log log = LogFactory.getLog(Mapper.class);
	
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
		{
			
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			IntWritable friend1 = new IntWritable();
			IntWritable followed = new IntWritable();
			IntWritable user = new IntWritable();
			ArrayList<Integer> frinedList = new ArrayList<>();
			user.set(Integer.parseInt(itr.nextToken()));
			while (itr.hasMoreTokens())
			{
				// convert all the string tokens to integer
				frinedList.add(Integer.parseInt(itr.nextToken()));
			}

			for (Integer friend : frinedList)
			{
			    friend1.set(friend);
				context.write(friend1, user);// emit friend as a key and user as a value 
				followed.set(-friend);// use the - trick to keep track on people followed by user
				context.write(user, followed);
			}
		}
	}

	

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>

	{
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<Integer> existingFriends = new ArrayList();
			IntWritable temp = new IntWritable(1);
			// int counter=0;
			int value;
			while (values.iterator().hasNext())
			{
				value = values.iterator().next().get();
				existingFriends.add(value);
				// counter++;
			}

			
			context.write(key, temp);
		}
	}

	/* The Mapper for the second MapReduce job */

	public static class MapRecommendation extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>

	{

	
		public void map(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException

		{

		

			IntWritable friend1 = new IntWritable(1);
			IntWritable friend2 = new IntWritable(2);
			Text word = new Text();
			word.set("Raheel");

			

			context.write(friend1, friend2);

		}

	}

	/* The Reducer for the second MapReduce job */

	public static class ReduceRecommendation extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public final Log logR = LogFactory.getLog(ReduceRecommendation.class);

		public void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException

		{

			// ArrayList<Integer> existingFriends = new ArrayList();
			// ArrayList<Integer> recommendedUsers = new ArrayList<>();
			IntWritable friend1 = new IntWritable(1);
			IntWritable friend2 = new IntWritable(2);
			context.write(friend1, friend2);

		}

	}

}
