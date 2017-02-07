
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
public class WhoToFollow extends Configured implements Tool

{

	public static void main(String[] args) throws Exception

	{
		if (args.length != 2)
////
		{
			System.out.println("Usage: FriendRecommendation <input dir> <output dir> \n");
			System.exit(-1);
		}

		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new WhoToFollow(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		@SuppressWarnings("deprecation")
		String intermediateFileDir = "tmp1";
		String intermediateFileDirFile = intermediateFileDir + "/part-r-00000";
		JobControl control = new JobControl("ChainMapReduce");
		ControlledJob step1 = new ControlledJob(jobListFriends(args[0], intermediateFileDir), null);
		ControlledJob step2 = new ControlledJob(jobRecommendFriends(intermediateFileDirFile, args[1]),Arrays.asList(step1));
		control.addJob(step1);
		control.addJob(step2);
		Thread workFlowThread = new Thread(control, "workflowthread");
		workFlowThread.setDaemon(true);
		workFlowThread.start();
		return 0;

	}

	/*
	 * The code above is the driver for the chained MapReduce jobs. The file
	 * output path for MapReduce job 1 must match exactly with the file input
	 * path for MapReduce job 2. Another critical issue is that we need to
	 * specify the dependency in "ControlledJob step2" so that MapReduce job 2
	 * will only start to run after the first MapReduce job completes.
	 */

	/* The code above setup all the configuration for MapReduce job 1. */

	private Job jobListFriends(String inputPath, String outputPath)
			throws IOException, InterruptedException, ClassNotFoundException {

		Job job = new Job();

		job.setJarByClass(WhoToFollow.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class); // Need to																												
		job.setOutputFormatClass(TextOutputFormat.class);// ???
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		return job;
	}

	/* The code above setup all the configuration for MapReduce job 2. */

	private Job jobRecommendFriends(String inputPath, String outputPath)
			throws IOException, InterruptedException, ClassNotFoundException {

		@SuppressWarnings("deprecation")
		Job job1 = new Job();
		job1.setJarByClass(WhoToFollow.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(MapRecommendation.class);
		job1.setReducerClass(ReduceRecommendation.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(outputPath));
		job1.waitForCompletion(true);
		return job1;
	}

	/*
	 * The code above is the Mapper for the first MapReduce job.
	 * 
	 * We need add a mark "zero" here if the pair <userID1, userID2>
	 * 
	 * are found to be friends already which will be discared in the Reducer
	 * stage.
	 */

	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
		public final Log log = LogFactory.getLog(Mapper.class);
		@Override

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			log.info("in side the map ");
			StringTokenizer itr = new StringTokenizer(value.toString());
			IntWritable friend1 = new IntWritable();
			// IntWritable friend2 = new IntWritable();
			IntWritable followed = new IntWritable();
			IntWritable user = new IntWritable();
			ArrayList<Integer> frinedList = new ArrayList<>();
			user.set(Integer.parseInt(itr.nextToken()));
			while (itr.hasMoreTokens())
			{
				frinedList.add(Integer.parseInt(itr.nextToken()));
			}

			for (Integer friend : frinedList)
			{
			friend1.set(friend);
				context.write(friend1, user);
				followed.set(-friend);
				context.write(user, followed);
			}
		}
	}

	/*
	 * The code is the Reducer for the first MapReduce job.
	 * 
	 * We need delete the pair <userID1, userID2>
	 * 
	 * which are found to be friends already and marked with "Zero"
	 * 
	 * in the value of the input in the Mapper stage.
	 */

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

			/*
			 * IntWritable[] temp1 = new IntWritable[existingFriends.size()];
			 * 
			 * for(int i=0 ; i<temp1.length;i++)
			 * 
			 * {
			 * 
			 * temp.set(existingFriends.get(i));
			 * 
			 * temp1[i] = temp;
			 * 
			 * }
			 */
			context.write(key, temp);
		}
	}

	/* The Mapper for the second MapReduce job */

	public static class MapRecommendation extends Mapper<Text, Text, IntWritable, IntWritable>

	{

		public void map(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException

		{

			// ArrayList<Integer> existingFriends = new ArrayList();

			// ArrayList<Integer> recommendedUsers = new ArrayList<>();

			IntWritable friend1 = new IntWritable(1);
			IntWritable friend2 = new IntWritable(2);
			Text word = new Text();
			word.set("Raheel");

			/*
			 * IntWritable temp = new IntWritable();
			 * 
			 * IntWritable count = new IntWritable();
			 * 
			 * IntWritable length = new IntWritable();
			 */

			/*
			 * while (values.iterator().hasNext())
			 * 
			 * {
			 * 
			 * String value = values.iterator().next().get();
			 * 
			 * if (value > 0)
			 * 
			 * {
			 * 
			 * //logR.info("enter if " );
			 * 
			 * recommendedUsers.add(value);
			 * 
			 * } else
			 * 
			 * {
			 * 
			 * //logR.info("enter else " );
			 * 
			 * existingFriends.add(value);
			 * 
			 * }
			 * 
			 * }
			 */

			/*
			 * 
			 * for (int i = 0; i < recommendedUsers.size(); i++)
			 * 
			 * {
			 * 
			 * friend1.set(recommendedUsers.get(i));
			 * 
			 * for (int j = 0; j < recommendedUsers.size(); j++)
			 * 
			 * {
			 * 
			 * 
			 * 
			 * if (recommendedUsers.get(i)!= recommendedUsers.get(j))
			 * 
			 * {
			 * 
			 * friend2.set(recommendedUsers.get(j));
			 * 
			 * context.write(friend1, friend2);
			 * 
			 * }
			 * 
			 * }
			 * 
			 * }
			 */

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
