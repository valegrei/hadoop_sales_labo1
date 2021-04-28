package preg7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SameCoordsMapper extends Mapper<LongWritable,Text,Text,Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");

		//Preg5: Que personas se encuentran mas cercanos segun lat lgt

		if(!("Price").equals(SingleCountryData[2])){
			//IntWritable numero;
			String lat;
			String lgt;
			String name;

			if(("\"13").equals(SingleCountryData[2])) {
				name = SingleCountryData[5].toLowerCase().trim();
				lat = SingleCountryData[11].trim();
				lgt = SingleCountryData[12].trim();
			}else{
				name = SingleCountryData[4].toLowerCase().trim();
				lat = SingleCountryData[10].trim();
				lgt = SingleCountryData[11].trim();
			}
			context.write(new Text(lat + " - "+lgt), new Text(name));
		}
	}
}
