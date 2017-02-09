import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public  class SecondMap extends Mapper<Object, Text, IntWritable, IntWritable>

	{
		//public final Log logR = LogFactory.getLog(ReduceRecommendation.class);

		public void map(Object key, Text values, Context context) //VHS - modified
				throws IOException, InterruptedException

		{
			//logR.info("Seecond Map");
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

			//logR.info(" second map _values" + test_input.toString());

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