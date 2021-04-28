package preg7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SameCoordsReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		List<String> names = new LinkedList<>();

		// Se obtiene los subtotales por persona
		for (Text value : values) {
			String name = value.toString();
			names.add(name);
			count++;
		}

		if (count>1){
			Text res = new Text(names.stream().collect(Collectors.joining(" ")));
			context.write(key, res);
		}

	}

}
