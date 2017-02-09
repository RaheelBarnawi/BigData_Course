import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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
	 //*****************************************************************************
	private static Map<Integer, Integer> sortByCommonFriend(Map<Integer, Integer> unsortMap) {
	        // 1. Convert Map to List of Map
	        List<Map.Entry<Integer, Integer>> recommendedFriendlist =
	                new LinkedList<Map.Entry<Integer, Integer>>(unsortMap.entrySet());
	        // 2. Sort list with Collections.sort(), provide a custom Comparator
	        Collections.sort(recommendedFriendlist, new Comparator<Map.Entry<Integer, Integer>>() {
	            public int compare(Map.Entry<Integer, Integer> object1,
	                              Map.Entry<Integer, Integer> object2) {
	                return (object1.getValue()).compareTo(object2.getValue());
	            }
	        });
	        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
	        Map<Integer, Integer> sortedMap = new LinkedHashMap<Integer, Integer>();
	        for (Map.Entry<Integer, Integer> entry : recommendedFriendlist) {
	            sortedMap.put(entry.getKey(), entry.getValue());}
	        return sortedMap;
	    }
	    public static <K, V> String printMap(Map<K, V> map) 
	    {
	    StringBuffer sb = new StringBuffer("");
	    String s= "";
	        for (Map.Entry<K, V> entry : map.entrySet()) 
	        { s=   (" " + entry.getKey()  + " (" + entry.getValue()+ ")");
	        sb.append(s + " ");
	        }
	        return sb.toString();
	}

	//**********************************************************************
	    
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
			
			// sorts the resulting recommendation 
			 Map<Integer, Integer> sortedMap = sortByCommonFriend(recommendedmap);
		        String sortedResult= printMap(sortedMap);
		        Text r= new Text();
		        r.set(sortedResult);
		        context.write(user, r);
		/*	StringBuffer sb = new StringBuffer("");
			for (Integer name : recommendedmap.keySet()) {

				String keyMap = name.toString();
				String valueMap = recommendedmap.get(name).toString();
				String resultMap = keyMap + "(" + valueMap + ")";
				sb.append(resultMap + " ");

			}
			Text result = new Text(sb.toString());
			context.write(user, result);
			*/

		}
		}