package preg11;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VentasCiudadMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");

		//Preg11: Sumar todas las ventas totales por tipo de pago en cada Ciudad

		if(!("Price").equals(SingleCountryData[2])){
			IntWritable numero;
			String tipoPago;
			String ciudad;

			if(("\"13").equals(SingleCountryData[2])) {
				numero = new IntWritable(13000);
				tipoPago = SingleCountryData[4].trim();
				ciudad = SingleCountryData[6].trim();
			}else{
				numero = new IntWritable(Integer.parseInt(SingleCountryData[2]));
				tipoPago = SingleCountryData[3].trim();
				ciudad = SingleCountryData[5].trim();
			}
			context.write(new Text(ciudad + " - "+tipoPago), numero);
		}
	}
}
