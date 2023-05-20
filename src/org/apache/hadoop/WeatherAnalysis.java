package org.apache.hadoop;

//Importing Libraries:
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

//The main class for the program:
public class WeatherAnalysis {
	//for missing or miss information or damaged data:
	static float maximum_temp_on_earth=(float) 56.7;
	static float minimum_temp_on_earth=(float) -89.2;
	// TemperatureMapper class:
	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\\s+");

			if (fields.length >= 8) {
				float temperature = Float.parseFloat(fields[10]);
				//Handling the inconsistency information/temperature readings from dataset:
				if(maximum_temp_on_earth>=temperature && minimum_temp_on_earth<=temperature) {
					context.write(new Text("Temperature"), new FloatWritable(temperature));
				}
			}
		}
	}

	// TemperatureReducer class:
	public static class TemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		@Override
	protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

			float sum = 0.0f;
			int count = 0;
			float min = Float.MAX_VALUE;
			float max = Float.MIN_VALUE;

			for (FloatWritable value : values) {
				float temperature = value.get();
				sum += temperature;
				count++;

				if (temperature < min) {
					min = temperature;
				}

				if (temperature > max) {
					max = temperature;
				}
			}

			float avg = sum / count;
			context.write(new Text("Minimum Temperature"), new FloatWritable(min));
			context.write(new Text("Maximum Temperature"), new FloatWritable(max));
			context.write(new Text("Average Temperature"), new FloatWritable(avg));
		}
	}

	// Main Program(Driver):
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "weather analysis");
		job.setJarByClass(WeatherAnalysis.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}