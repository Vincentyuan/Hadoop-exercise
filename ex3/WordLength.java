import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLength {

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private IntWritable length = new IntWritable();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      //the output key should be intwritable and the output is the word. 
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
	

	//System.out.println(word.toString().lengot());
	length.set(word.toString().length());
        context.write(length,word);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String result = "";
      List<String> list  = new ArrayList<String>();

      for (Text txt : values) {
	//        result += txt.toString()+" ";
	list.add(txt.toString());
      }
      Collections.sort(list);
      for(String s : list){
	result += s+" ";	
      }
      
      context.write(key, new Text(result));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordLength.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
