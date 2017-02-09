import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public  class SecondMap extends Mapper<Object, Text, IntWritable, IntWritable>

	{
		//public final Log logR = LogFactory.getLog(ReduceRecommendation.class);

		public void map(Object key, Text values, Context context) 
				throws IOException, InterruptedException

		{
			StringTokenizer itr = new StringTokenizer(values.toString());
			ArrayList<Integer> existingFriends = new ArrayList();
			ArrayList<Integer> recommendedUsers = new ArrayList<>();
			IntWritable friend1 = new IntWritable();
			IntWritable friend2 = new IntWritable();
			IntWritable existingFriend = new IntWritable();
			ArrayList<Integer> frinedList = new ArrayList<>();
			Integer userKey = new Integer(itr.nextToken());
			while (itr.hasMoreTokens()) 
			{frinedList.add(Integer.parseInt(itr.nextToken()));// converts all tokens to type Integer
			}
			
			for (Integer friend : frinedList)
			{
				if (friend > 0)
					recommendedUsers.add(friend);
				else
					existingFriends.add(friend);
			}

			
			for (int i = 0; i < recommendedUsers.size(); i++)
			{
				friend1.set(recommendedUsers.get(i));
				for (int j = 0; j < recommendedUsers.size(); j++) 
				{
					if (recommendedUsers.get(i) != recommendedUsers.get(j)) 
					{
						friend2.set(recommendedUsers.get(j));
						context.write(friend1, friend2);// emit (a,b)(b,a) 
					}
				}
			}
			for (int i = 0; i < existingFriends.size(); i++) 
			{
				existingFriend.set(existingFriends.get(i));
				context.write(new IntWritable(userKey), existingFriend);// emit existingFriend 
			}
		}

	}