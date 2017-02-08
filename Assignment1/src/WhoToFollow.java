
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;



import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

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
	  Configuration conf = getConf();
	 FileSystem fs = FileSystem.get(conf);
	  Job job = new Job(conf, "Job1");
	  job.setJarByClass(WhoToFollow.class);
	  log.info(" job 0");
	  job.setMapperClass(Map1.class);
	  job.setReducerClass(Reduce1.class);
	  log.info(" job 1");
	  //reducer output(k,v) classes 
	  job.setOutputKeyClass(IntWritable.class);
	  job.setOutputValueClass(IntWritable.class);
	  log.info("job 2");
	// mapper's output (K,V) classes
	  job.setMapOutputKeyClass(IntWritable.class);
	  job.setMapOutputValueClass(IntWritable.class);
	  log.info("job 3");
	  job.setInputFormatClass(TextInputFormat.class);
	  job.setOutputFormatClass(SequenceFileOutputFormat.class);
	 

	 FileInputFormat.setInputPaths(job, new Path(args[0]));
	  TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	  job.waitForCompletion(true);

	  /*
	   * Job 2
	   */
	  
	  Job job2 = new Job(conf, "Job 2");
	  job2.setJarByClass(WhoToFollow.class);
	  log.info("job1 0");
	  job2.setMapperClass(MapRecommendation.class);
	  job2.setReducerClass(ReduceRecommendation.class);
	  log.info("job1 1");
	  //reducer output(k,v) classes 
	  job2.setOutputKeyClass(IntWritable.class);
	  job2.setOutputValueClass(IntWritable.class);
	  log.info("job1 2");
	// mapper's output (K,V) classes
	  job2.setMapOutputKeyClass(IntWritable.class);
	  job2.setMapOutputValueClass(IntWritable.class);
	  log.info("job1 3");

	 job2.setInputFormatClass(SequenceFileInputFormat.class);
	 job2.setOutputFormatClass(TextOutputFormat.class);
	 log.info("job1 4");
	  FileInputFormat.setInputPaths(job2, new Path(OUTPUT_PATH));
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

	

	public static class Map1 extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		public final Log log = LogFactory.getLog(Map1.class);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			//log.info("mapper");
			
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

	

	public static class Reduce1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>

	{
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//ArrayList<Integer> existingFriends = new ArrayList();
			IntWritable temp = new IntWritable();
			int value;
			while (values.iterator().hasNext())
			{
				value = values.iterator().next().get();
				temp.set(value);
				context.write(key, temp);
				// counter++;
			}

			
			
		}
	}

	/* The Mapper for the second MapReduce job */

	public static class MapRecommendation extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>

	{

	
		public void map(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException

		{
			ArrayList<Integer> existingFriends = new ArrayList();
			ArrayList<Integer> recommendedUsers = new ArrayList<>();
			IntWritable friend1 = new IntWritable();
			IntWritable friend2 = new IntWritable();
			IntWritable temp = new IntWritable();
			while (values.iterator().hasNext()) {
				int value = values.iterator().next().get();
				// logR.info("print value"+ value );
				if (value > 0)
					recommendedUsers.add(value);
				else
					existingFriends.add(value);
			}
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
				context.write(key, temp);
			}
		}

	}

	/* The Reducer for the second MapReduce job */

	public static class ReduceRecommendation extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public final Log logR = LogFactory.getLog(ReduceRecommendation.class);
		
		//**************************************************************
		// A private class to describe a recommendation.
        // A recommendation has a friend id and a number of friends in common.
        private static class Recommendation {

            // Attributes
            private int friendId;
            private int nCommonFriends;

            // Constructor
            public Recommendation(int friendId) {
                this.friendId = friendId;
                // A recommendation must have at least 1 common friend
                this.nCommonFriends = 1;
            }

            // Getters
            public int getFriendId() {
                return friendId;
            }

            public int getNCommonFriends() {
                return nCommonFriends;
            }

            // Other methods
            // Increments the number of common friends
            public void addCommonFriend() {
                nCommonFriends++;
            }

            // String representation used in the reduce output            
            public String toString() {
                return friendId + "(" + nCommonFriends + ")";
            }

            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                // Recommendation was not found!
                return null;
            }
        }
		//**************************************************************

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException

		{
			IntWritable temp = new IntWritable();
			int value;
			while (values.iterator().hasNext())
			{
				value = values.iterator().next().get();
				temp.set(value);
				context.write(key, temp);
				// counter++;
			}

			// ArrayList<Integer> existingFriends = new ArrayList();
			// ArrayList<Integer> recommendedUsers = new ArrayList<>();
			///IntWritable friend1 = new IntWritable(1);
			//IntWritable friend2 = new IntWritable(2);
			//context.write(friend1, friend2);

		}

	}

}
