import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FileJoin {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

      // display the name of the input file
      String inputFile = ((FileSplit) context.getInputSplit()).getPath().getName();
      System.out.println("\n\ninputFile=" + inputFile + "\n\n");

      // NOTE: a value is of type Text (not IntWritable as in the WordCount application)

      StringTokenizer tokens = new StringTokenizer(value.toString());
//	System.out.println("tokens : "+tokens.nextToken()+" next token :"+tokens.nextToken());
      // process the input file
      if (inputFile.charAt(0) == 'R') {
        // input file is Rxyz
      	while(tokens.hasMoreTokens()){
		//System.out.println("sdsda"+tokens.nextToken());
 //     		String [] list = tokens.nextToken().split(" ");
      	//	Context temp = new Context();
      	//	temp.write(list[0],"r");
		String first = tokens.nextToken();
		String second  = tokens.nextToken();
    System.out.println(" "+second+" "+first);
      		context.write(new Text(second),new Text(first+" R"));
      	}
      } else {
        // input file is Suvw
      	while(tokens.hasMoreTokens()){
                    //  String [] list = tokens.nextToken().split(" ");
                    //  Context temp = new Context();
                    //  temp.write(list[0],"r");
		    String first = tokens.nextToken();
		    String second = tokens.nextToken();
        System.out.println(" "+second+" "+first);
                     context.write(new Text(first),new Text(second+" S"));
              }
      }

    }
  }

  public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

      // NOTE: values are of type Text
      String result = "";
      List<String> listR = new ArrayList<String>();
      List<String> listS = new ArrayList<String>();
      for(Text t : values)
    	{
    		String tmp = t.toString();
        String [] separate = tmp.split(" ");
        if(separate[1].equals("S")){
          listS.add(separate[0]);
        }else
          listR.add(separate[0]);
    	}
      for(int i =0 ; i < listR.size();i++){
        for(int j =0 ; j<listS.size();j++){
          result = listR.get(i)+" "+key.toString()+" "+listS.get(j);
        context.write(key,new Text(result));
        }
      }
      // process the values associated with the key

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "file join");
    job.setJarByClass(FileJoin.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(JoinReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
