
package preg7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinDistanceDriver {
	public static void main(String[] args) throws Exception {

		/* JOB 1 */
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "mindistance");

		job1.setJarByClass(MinDistanceDriver.class);
		job1.setMapperClass(SameCoordsMapper.class);
		job1.setReducerClass(SameCoordsReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1])); //Output of job1
		//job1.waitForCompletion(true);
		System.exit(job1.waitForCompletion(true) ? 0:1);

		/* JOB 2
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "min_max");

		job2.setJarByClass(MinMaxDriver.class);
		job2.setMapperClass(MinMaxMapper.class);
		job2.setReducerClass(MinMaxReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2])); //Output of job1
		System.exit(job2.waitForCompletion(true) ? 0:1);*/
	}
}
