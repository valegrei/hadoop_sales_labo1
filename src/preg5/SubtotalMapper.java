package preg5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;

public class SubtotalMapper extends Mapper<LongWritable,Text,Text,Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");

		//Preg5: Que personas de cada pais tienen mas y menor monto en compras.

		if(!("Price").equals(SingleCountryData[2])){
			//IntWritable numero;
			String price;
			String name;
			String pais;

			if(("\"13").equals(SingleCountryData[2])) {
				price = (new Integer(13000)).toString();
				name = SingleCountryData[5].toLowerCase().trim();
				pais = SingleCountryData[8].trim();
			}else{
				price = SingleCountryData[2]; //Precio
				name = SingleCountryData[4].toLowerCase().trim(); //Nombre
				pais = SingleCountryData[7].trim(); // Pais
			}
			context.write(new Text(pais), new Text(name+","+price));
		}
	}
}
