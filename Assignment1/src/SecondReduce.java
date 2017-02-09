import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;


public  class SecondReduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		//public final Log logR = LogFactory.getLog(ReduceRecommendation.class);

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			IntWritable user = key;
			// 'existingFriends' will store the friends of user 'user'
			// (the negative values in 'values').
			ArrayList<Integer> existingFriends = new ArrayList();
			ArrayList<Integer> recommendedUsers = new ArrayList<>();//'recommendedUsers' will store the list of user ids recommended to user 'user'
			while (values.iterator().hasNext()) 
			{
				int value = values.iterator().next().get();
				if (value > 0) 
					recommendedUsers.add(value);
				else 
					existingFriends.add(value);
				
			}

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