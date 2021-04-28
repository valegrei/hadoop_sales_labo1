package preg5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SubtotalReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Map<String,Integer> mapa = new HashMap<>();

		// Se obtiene los subtotales por persona
		for (Text value : values) {
			// replace type of value with the actual type of our value
			String[] tokens = value.toString().split(",");	//Name,Price
			String name = tokens[0];
			int monto = mapa.getOrDefault(name, 0);
			monto += Integer.parseInt(tokens[1]);
			mapa.put(name,monto);
		}

		// Se busca las personas con min o max
		int min = Integer.MAX_VALUE; int max = -1;
		Map.Entry<String,Integer> minEntry=null,maxEntry=null;
		for (Map.Entry<String,Integer> entry : mapa.entrySet()){
			if(entry.getValue() < min){
				minEntry = entry;
				min = minEntry.getValue();
			}
			if(entry.getValue() > max){
				maxEntry = entry;
				max = maxEntry.getValue();
			}
		}

		Text res = new Text(minEntry.getKey()+":"+minEntry.getValue()+" "+maxEntry.getKey()+":"+maxEntry.getValue());

		context.write(key, res);
	}

}
