
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
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
		// log.info(" job 0");
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		// log.info(" job 1");
		// reducer output(k,v) classes
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		// log.info("job 2");
		// mapper's output (K,V) classes
		//job.setMapOutputKeyClass(IntWritable.class);
	    //job.setMapOutputValueClass(IntWritable.class);
		// log.info("job 3");
		//job.setInputFormatClass(Text.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputKeyClass(IntWritable.class);
       // job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		job.waitForCompletion(true);

		/*
		 * Job 2
		 */

		Configuration conf1 = new Configuration();//getConf();
		Job job2 = Job.getInstance(conf1, "Job 2");
		job2.setJarByClass(WhoToFollow.class);
		// log.info("job1 0");
		job2.setMapperClass(MapRecommendation.class);
		job2.setReducerClass(ReduceRecommendation.class);
		// log.info("job1 1");
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		// log.info("job1 2");
		// mapper's output (K,V) classes
		//job2.setMapOutputKeyClass(IntWritable.class);
	    //job2.setMapOutputValueClass(IntWritable.class);
		// log.info("job1 3");

		//job2.setInputFormatClass(SequenceFileInputFormat.class);
		//job2.setOutputFormatClass(TextOutputFormat.class);
		// log.info("job1 4");
		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

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
		ToolRunner.run(new Configuration(), new WhoToFollow(), args);
	}

	public static class Map1 extends Mapper<Object, Text, IntWritable, IntWritable> {
		public final Log log = LogFactory.getLog(Map1.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			IntWritable friend1 = new IntWritable();
			IntWritable followed = new IntWritable();
			IntWritable user = new IntWritable();
			ArrayList<Integer> frinedList = new ArrayList<>();
			user.set(Integer.parseInt(itr.nextToken()));
			while (itr.hasMoreTokens()) {
			
				frinedList.add(Integer.parseInt(itr.nextToken()));
			}

			for (Integer friend : frinedList) {
				friend1.set(friend);
				context.write(friend1, user);// emit friend as a key and user as
												// a value
				followed.set(-friend);// use the - trick to keep track on people
										// followed by user
				context.write(user, followed);
			}
		}
	}

	public static class Reduce1 extends Reducer<IntWritable, IntWritable, IntWritable, Text>

	{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// ArrayList<Integer> existingFriends = new ArrayList();
			IntWritable temp = new IntWritable();
			String value = "";
			while (values.iterator().hasNext()) {
				value += values.iterator().next().get() + " ";
				//temp.set(value);
				

			}
			context.write(key, new Text(value));

		}
	}

	/* The Mapper for the second MapReduce job */

	public static class MapRecommendation extends Mapper<Object, Text, IntWritable, IntWritable>

	{
		public final Log logR = LogFactory.getLog(ReduceRecommendation.class);

		public void map(Object key, Text values, Context context) //VHS - modified
				throws IOException, InterruptedException

		{
			logR.info("Seecond Map");
			StringTokenizer itr = new StringTokenizer(values.toString());
			ArrayList<Integer> existingFriends = new ArrayList();
			ArrayList<Integer> recommendedUsers = new ArrayList<>();
			IntWritable friend1 = new IntWritable();
			IntWritable friend2 = new IntWritable();
			IntWritable temp = new IntWritable();
			ArrayList<Integer> frinedList = new ArrayList<>();
			ArrayList<Integer> test_input = new ArrayList<Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String toString() {
					return super.toString();
				}
			};
			
			Integer userKey = new Integer(itr.nextToken());//VHS - modified

			while (itr.hasMoreTokens()) {
				
				frinedList.add(Integer.parseInt(itr.nextToken()));
			}
			for (Integer friend : frinedList){
				test_input.add(friend);
				// logR.info("print value"+ value );
				if (friend > 0)
					recommendedUsers.add(friend);
				else
					existingFriends.add(friend);
			}

			logR.info(" second map _values" + test_input.toString());

			for (int i = 0; i < recommendedUsers.size(); i++) {
				friend1.set(recommendedUsers.get(i));
				for (int j = 0; j < recommendedUsers.size(); j++) {
					if (recommendedUsers.get(i) != recommendedUsers.get(j)) {
						friend2.set(recommendedUsers.get(j));
						context.write(friend1, friend2);
					}
				}
			}
			for (int i = 0; i < existingFriends.size(); i++) {
				temp.set(existingFriends.get(i));
				context.write(new IntWritable(userKey), temp);//VHS - modified
			}
		}

	}

	/* The Reducer for the second MapReduce job */

	public static class ReduceRecommendation extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		public final Log logR = LogFactory.getLog(ReduceRecommendation.class);

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			IntWritable user = key;
			// 'existingFriends' will store the friends of user 'user'
			// (the negative values in 'values').
			ArrayList<Integer> existingFriends = new ArrayList();
			ArrayList<Integer> test_input = new ArrayList<Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String toString() {
					return super.toString();
				}
			};

			// 'recommendedUsers' will store the list of user ids recommended
			// to user 'user'
			ArrayList<Integer> recommendedUsers = new ArrayList<>();
			while (values.iterator().hasNext()) {
				int value = values.iterator().next().get();
				test_input.add(value);
				if (value > 0) {
					recommendedUsers.add(value);
				} else {
					existingFriends.add(value);
				}
			}
			logR.info(" second_reducer_value" + test_input.toString());

			for (final Integer friend : existingFriends) {
				recommendedUsers.removeIf(new Predicate<Integer>() {
					@Override
					public boolean test(Integer t) {
						return t.intValue() == -friend.intValue();
					}
				});
			}

			// find number of common friends using hash map key is the
			// recomanded user - value is number of friends in common
			Map<Integer, Integer> recommendedmap = new HashMap<Integer, Integer>();

			for (Integer a : recommendedUsers) {
				if (recommendedmap.containsKey(a)) {
					recommendedmap.put(a, recommendedmap.get(a) + 1);
				} else {
					recommendedmap.put(a, 1);
				}
			}
			StringBuffer sb = new StringBuffer("");
			for (Integer name : recommendedmap.keySet()) {

				String keyMap = name.toString();
				String valueMap = recommendedmap.get(name).toString();
				String resultMap = keyMap + "(" + valueMap + ")";
				sb.append(resultMap + " ");

			}
			Text result = new Text(sb.toString());
			context.write(user, result);

		}

	}

}
