package preg1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	private IntWritable numero;

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");

		//Preg1: Contar el numero de ventas por tipo de pago por cada ciudad
		output.collect(new Text(SingleCountryData[5].trim() + " - " + SingleCountryData[3].trim() ), one); //Ciudad - Tipo Pago
	}
}
