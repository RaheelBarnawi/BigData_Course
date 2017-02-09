import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;


public  class FirstReduce extends Reducer<IntWritable, IntWritable, IntWritable, Text>

{public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {

		String value = "";
		while (values.iterator().hasNext())
		{
			value += values.iterator().next().get() + " ";
		}
		context.write(key, new Text(value)); // emit inverted lists

	}
}