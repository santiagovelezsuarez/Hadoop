import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Friends 
{
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
  {    
    private Text comunes = new Text();
    private Text friendss = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {        
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) 
        {
            String line = itr.nextToken();
            String comun = line.substring(0,line.indexOf("->"));
            String friends = line.substring(line.indexOf("->")+3);
            
            String friend[] = friends.split(" ");
            for(int i=0; i<friend.length; i++)
            {
                for(int j=i; j<friend.length; j++)
                {
                    if(i!=j)
                    {
                        comunes.set(friend[i]+","+friend[j]+" -> ");
                        friendss.set(comun);
                        context.write(comunes, friendss);
                        //System.out.println("("+friend[i]+","+friend[j]+") : ["+comun+"]");
                    }                        
                }
            }
            //word.set(itr.nextToken());
            //context.write(word, one);
        }
    }
  }

  public static class SumReducer extends Reducer<Text,Text,Text,Text>
  {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
      String concat = "";
      for (Text val : values)
      {
        concat += val.toString();
      }
      result.set(concat);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "friends");
    job.setJarByClass(Friends.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
