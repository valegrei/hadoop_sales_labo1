package preg11;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class VentasCiudadReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int frequency = 0;
		List<String> names = new LinkedList<>();

		// Se obtiene los subtotales por persona
		for (IntWritable value : values) {
			frequency += value.get();
		}

		context.write(key, new IntWritable(frequency));
	}

}
