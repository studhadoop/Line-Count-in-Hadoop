package optimized;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineCntMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	Text keyEmit = new Text("Total Lines");
	IntWritable valEmit = new IntWritable();
	int partialSum = 0;

	public void map(LongWritable key, Text value, Context context) {
		partialSum++;
	}

	public void cleanup(Context context) {
		valEmit.set(partialSum);
		try {
			context.write(keyEmit, valEmit);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}
}
