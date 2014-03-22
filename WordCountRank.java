//input format is whole file input - custom format
import input.WholeFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import java.lang.InterruptedException; 
import java.util.StringTokenizer;
import java.util.*;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.io.IOException;
/**
 * @author  krithika
 * Rank top 3 webpages (wikipedia 50 states html webpages) wrt 
 * count of following words on each of those webpages
 * agriculture
 * politics
 * sports
 * education
 */
public class WordCountRank extends Configured
 implements Tool {

    public static Logger log = Logger.getLogger(WordCountRank.class);
    private static final Pattern UNDESIRABLES = Pattern.compile("[(){},.;!+\"?<>%]");

    //mapper class
    public static class WCRankMapper
       extends Mapper<Object, BytesWritable , Text, MapWritable>{

        private Text filenameKey;
        private MapWritable pairs;
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String elements[] = { "education", "politics", "sports", "agriculture" }; 
        private HashSet<String> dict = new HashSet<String>(Arrays.asList(elements));

        @Override
        protected void setup(Context context) throws IOException,
        InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            filenameKey = new Text(path.toString());
        }

        public void map(Object key, BytesWritable value, Context context
                    ) throws IOException, InterruptedException {
	          byte[] bytes = value.getBytes();
            String str = new String(bytes);
            //default string parser to parse word
            StringTokenizer itr = new StringTokenizer(str.toLowerCase());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
	           //remove undesirable characters to count words that start/end with special characters
                String cleanWord = UNDESIRABLES.matcher(word.toString()).replaceAll("");
                if(dict.contains(cleanWord)) {
                    pairs = new MapWritable();
		            pairs.put(filenameKey, one);
        	        context.write(new Text(cleanWord), pairs);
                }
            }
        }
    }

    //reducer class
    public static class WCRankReducer
      extends Reducer<Text,MapWritable,Text, Text> {

        private MapWritable result = new MapWritable();

        public void reduce(Text key, Iterable<MapWritable> values,
                   Context context
                   ) throws IOException, InterruptedException {

            for (MapWritable value : values) {
                addAll(value);
            }

          	Text fileName = new Text();
          	for(int i=0; i < 3; i++){
          	    int big=0;
          	    Iterator it= result.entrySet().iterator();
          	    while(it.hasNext()){
          	        Map.Entry<Text, IntWritable> entry=  (Map.Entry<Text, IntWritable>)it.next();
                    if(big < entry.getValue().get()) {
          		          big = entry.getValue().get();
          		          fileName = entry.getKey();
                    }
                }
                //logging to local logs of reducer
                //can check the log files from the UI web manager
                log.info("Executing for loop: "+i+"th time");
                log.info("Big: "+big+"\tfileName: "+fileName);
                result.remove(fileName);
                context.write(key,new Text(fileName.toString() + ": " + big));
            }

        }
        /**
         * count words and add if they belong to same file
         */
        private void addAll(MapWritable mapWritable) {
            Set<Writable> keys = mapWritable.keySet();
            
            for (Writable key : keys) {
                IntWritable fromCount = (IntWritable) mapWritable.get(key);
                if (result.containsKey(key)) {
                    IntWritable count = (IntWritable) result.get(key);
                    count.set(count.get() + fromCount.get());
                } else {
                    result.put(key, fromCount);
                }
            }
        }
    }


    public int run(String[] args) throws Exception {

        // Configuration processed by ToolRunner
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "word count");
        if (job == null) {
          return -1;
        }

        job.setJarByClass(WordCountRank.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setMapperClass(WCRankMapper.class);
        job.setReducerClass(WCRankReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountRank(), args);
        System.exit(exitCode);
    }
}

