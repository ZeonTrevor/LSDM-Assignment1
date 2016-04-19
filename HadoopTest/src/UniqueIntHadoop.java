import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UniqueIntHadoop {
    static class NumberMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
        private static List<String> numbers = Arrays.asList("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int mapped = numbers.indexOf(value.toString());
            context.write(new IntWritable(mapped), NullWritable.get());
        }
    }

   /*//Important Property is that the input types of key/value pairs and output types of key/values should be same
    static class Combiner extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
    	  @Override
    	  protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
    	   context.write(key, NullWritable.get());
    	  }
    }*/
    
    static class UniqueIntReducer extends Reducer<IntWritable, NullWritable, NullWritable, IntWritable> {
    	@Override
    	protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
    		context.write(NullWritable.get(), key);
    	}
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(UniqueIntHadoop.class);
        
        job.setMapperClass(NumberMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(UniqueIntReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //job.setCombinerClass(Combiner.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path("data/duplicatenumbers.txt"));
        TextOutputFormat.setOutputPath(job, new Path("results/hadoop/numbersunique"));

        job.waitForCompletion(true);
    }


}
