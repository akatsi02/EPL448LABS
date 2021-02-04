package Lab2;
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Exercise0 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable valOut; 
    private Text keyOut;
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split(",");
        String country = fields[7];
        int amount = Integer.parseInt(fields[2]);
        
        keyOut = new Text(country);
        valOut = new IntWritable(amount);
        	        
        context.write(keyOut, valOut);
        
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
    		throws IOException, InterruptedException {
    	
    	int numProducts = 0;
    	int sumAmount = 0;
    	
    	for (IntWritable v : values) {
    		numProducts++;
    		sumAmount += v.get();
    	}
    	String vOut = String.format("%d %d", numProducts, sumAmount);
    	
    	context.write(key, new Text(vOut));
    	
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = Job.getInstance(conf, "exercise0");
    job.setJarByClass(Exercise0.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    // job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/Lab_2/Exercise_0/SalesJan2009.csv"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/Lab_2/Exercise_0/out"));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}