package linecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LineCntMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	Text keyEmit = new Text("Total Lines");
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			 {
		try {
			context.write(keyEmit, one);
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
