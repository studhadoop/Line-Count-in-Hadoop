package optimized;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LineCntDriver extends Configured implements Tool {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		int res = 0;
		try {
			res = ToolRunner.run(conf, new LineCntDriver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(res);

	}

	@Override
	public int run(String[] args) {
		if (args.length != 2) {
			System.out.printf("Usage: Line count <input dir> <output dir>  \n");

			System.exit(-1);
		}

		String source = args[0];
		String dest = args[1];

		Configuration conf = new Configuration();
		Path in = new Path(source);
		Path out = new Path(dest);

		Job job = null;
		try {
			job = new Job(conf, "Line Count - Optimized");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		job.setJarByClass(LineCntDriver.class);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(LineCntMapper.class);
		job.setCombinerClass(LineCntReducer.class);
		job.setReducerClass(LineCntReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		try {
			TextInputFormat.addInputPath(job, in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		try {
			if(fs.exists(out)){
				fs.delete(out, true);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		TextOutputFormat.setOutputPath(job, out);
		boolean success = false;
		try {
			success = job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		return (success ? 0 : 1);

	}
}
