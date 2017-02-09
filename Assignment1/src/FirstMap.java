import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;


public  class FirstMap extends Mapper<Object, Text, IntWritable, IntWritable> {
		//public final Log log = LogFactory.getLog(Map1.class);

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