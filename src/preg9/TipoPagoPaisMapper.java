package preg9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TipoPagoPaisMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");

		//Preg5: Contar el número de ventas por tipo de pago en cada País

		if(!("Price").equals(SingleCountryData[2])){
			//IntWritable numero;
			String tipoPago;
			String pais;

			if(("\"13").equals(SingleCountryData[2])) {
				tipoPago = SingleCountryData[4].trim();
				pais = SingleCountryData[8].trim();
			}else{
				tipoPago = SingleCountryData[3].trim();
				pais = SingleCountryData[7].trim();
			}
			context.write(new Text(pais + " - "+tipoPago), one);
		}
	}
}
