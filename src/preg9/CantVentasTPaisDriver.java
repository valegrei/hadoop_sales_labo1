
package preg9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CantVentasTPaisDriver {
	public static void main(String[] args) throws Exception {

		/* JOB 1 */
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "mindistance");

		job1.setJarByClass(CantVentasTPaisDriver.class);
		job1.setMapperClass(TipoPagoPaisMapper.class);
		job1.setReducerClass(CantidadVentasReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1])); //Output of job1
		System.exit(job1.waitForCompletion(true) ? 0:1);

	}
}
