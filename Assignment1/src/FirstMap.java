import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// first step : Indexing:
public class FirstMap extends Mapper<Object, Text, IntWritable, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());
		IntWritable followed_by = new IntWritable();
		IntWritable followed = new IntWritable();
		IntWritable user = new IntWritable();
		ArrayList<Integer> frinedList = new ArrayList<>();
		user.set(Integer.parseInt(itr.nextToken())); // first token is the user X
		while (itr.hasMoreTokens()) 
		{
			frinedList.add(Integer.parseInt(itr.nextToken()));// convert all tokens to type Integer
			
		}

		for (Integer friend : frinedList) 
		{
			followed_by.set(friend);/// emit friend  as the key and user as the value
			context.write(followed_by, user);
			followed.set(-friend); // use the negative trick to keep track on user existing friends
			context.write(user, followed); 
		

		}
	}
}