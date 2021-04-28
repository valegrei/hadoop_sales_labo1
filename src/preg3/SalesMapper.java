package preg3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");

		//Preg3: Sumar todas las ventas totales por tipo de pago por cada Estado

		if(!("Price").equals(SingleCountryData[2])){
			IntWritable numero;
			String outKey;

			if(("\"13").equals(SingleCountryData[2])) {
				numero = new IntWritable(13000);
				outKey = SingleCountryData[7].trim() + " - " + SingleCountryData[4].trim();
			}else{
				numero = new IntWritable(Integer.parseInt(SingleCountryData[2])); //Precio
				outKey = SingleCountryData[6].trim() + " - " + SingleCountryData[3].trim(); // Estado - Tipo Pago
			}
			output.collect(new Text(outKey), numero);
		}

	}
}
