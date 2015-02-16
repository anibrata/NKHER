package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.nkher.HMapStIW;
import edu.umd.nkher.PairOfStrings;

public class StripesPMI_Seq extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  
  private static class WordCountSentenceMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Text WORD = new Text();
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      HashSet<String> hashSet = new HashSet<String>();
      String token = "";
      while (tokenizer.hasMoreTokens()) {
        token = tokenizer.nextToken();
        if (!hashSet.contains(token)) {
          hashSet.add(token);
          WORD.set(token);
          context.write(WORD, ONE);
        }
      }
    }
  }

  private static final class WordCountSentenceReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable TOTAL = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int total = 0;
      for (IntWritable iw : values) {
        total += iw.get();
      }
      TOTAL.set(total);
      context.write(key, TOTAL);
    }
  }

  private static class MyMapper extends
      Mapper<LongWritable, Text, Text, HMapStIW> {
    private final static HMapStIW hashMap = new HMapStIW();
    private final static Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      Set<String> sortedTokens = new TreeSet<String>();
      while (tokenizer.hasMoreTokens()) {
        sortedTokens.add(tokenizer.nextToken());
      }
      String[] lineWords = new String[sortedTokens.size()];
      sortedTokens.toArray(lineWords);

      int length = lineWords.length;
      for (int i = 0; i < length; i++) {
        String keyTerm = lineWords[i];
        if(keyTerm.length() == 0) {
        	continue;
        }
        hashMap.clear();
        for (int j = (i + 1); j < length; j++) {
        	if(lineWords[j].length() == 0) {
            	continue;
            }
          if (!hashMap.containsKey(lineWords[j])) {
            hashMap.put(lineWords[j], 1);
          }
        }
        KEY.set(keyTerm);
        context.write(KEY, hashMap);
      }
    }
  }

  private static class MyCombiner extends
      Reducer<Text, HMapStIW, Text, HMapStIW> {

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();
      while (iter.hasNext()) {
        map.plus(iter.next());
      }
      context.write(key, map);
    }
  }

  private static class MyReducer extends
      Reducer<Text, HMapStIW, PairOfStrings, DoubleWritable> {

    private static final PairOfStrings PAIR_OF_WORDS = new PairOfStrings();
    private static final DoubleWritable PMI = new DoubleWritable();
    private static final double N = 114863.0; // number of sentences
    private static HashMap<String, Integer> dictionary =
        new HashMap<String, Integer>();

    @Override
    public void setup(Context context) throws IOException {
      /*
       * now we have to populate the dictionary to get the individual word
       * counts which is the output of the first mapper
       */
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);

      String path = System.getProperty("user.dir");
      Path pathToFile = new Path("/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/stripesWordCount/part-r-00000");

      BufferedReader bufferedReader = null;
      FSDataInputStream fsdis = null;
      InputStreamReader isr = null;
      try {
        fsdis = fileSystem.open(pathToFile);
        isr = new InputStreamReader(fsdis);
        bufferedReader = new BufferedReader(isr);
      } catch (Exception e) {
        System.out
            .println("Some error occured while opening the file. Please check if the file is being properly made !");
      }
      try {
        String line = "";
        while ( (line = bufferedReader.readLine()) != null ) {
          String[] arr = line.split("\\s+");
          if (arr.length == 2) {
            dictionary.put(arr[0], Integer.parseInt(arr[1]));
          } else {
            LOG.info("Some error while creating dictionary.");
          }
        }
      }
	 catch (Exception e) {
	        LOG.info("Some error occured while reading the file");
	      }
      finally {
    	  bufferedReader.close();  
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {

    	Iterator<HMapStIW> iter = values.iterator();
        HMapStIW map = new HMapStIW();
        while (iter.hasNext()) {
          map.plus(iter.next());
        }
      
      String leftWordOfPair = key.toString();
      
      for (String rightWordOfPair : map.keySet()) {
        double p_x_y = map.get(rightWordOfPair);
        if (p_x_y >= 10) {
          PAIR_OF_WORDS.set(leftWordOfPair, rightWordOfPair);
          double numerator = p_x_y / N;
          double prob_occ_x = (double) dictionary.get(leftWordOfPair) / N;
          double prob_occ_y = (double) dictionary.get(rightWordOfPair) / N;
          double pmi = (double) Math.log10((numerator) / (prob_occ_x * prob_occ_y));
          PMI.set(pmi);
          context.write(PAIR_OF_WORDS, PMI);
        }
      }
    }
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    /* JOB ONE CONFIGURATION STARTS */

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    String path = System.getProperty("user.dir");
    String mapperOneOutputPath = "/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/stripesWordCount";
    
    Path path2 = new Path(mapperOneOutputPath);
    
    FileSystem fs = FileSystem.get(getConf());
    if(fs.exists(path2)) {
    	fs.delete(new Path(mapperOneOutputPath), true);
    }

    int reduceTasks =
        cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + StripesPMI_Seq.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + mapperOneOutputPath);
    LOG.info(" - num reducers: " + 1);

    Configuration customConfiguration = getConf();
    customConfiguration.set("mapOneOutput", mapperOneOutputPath);

    Job firstJob = Job.getInstance(customConfiguration);
    firstJob.setJobName("Sentence Wise Word Counter");
    firstJob.setJarByClass(StripesPMI_Seq.class);
    
    firstJob.setNumReduceTasks(1); // using one reducer for the first job

    FileInputFormat.setInputPaths(firstJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(firstJob, new Path(mapperOneOutputPath));

    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);

    firstJob.setMapperClass(WordCountSentenceMapper.class);
    //firstJob.setCombinerClass(WordCountSentenceReducer.class);
    firstJob.setReducerClass(WordCountSentenceReducer.class);

    Path outputmapperonePath = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputmapperonePath, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    /* JOB ONE CONFIGURATION ENDS */

    /* JOB TWO CONFIGURATION STARTS */

    LOG.info("Tool name: " + StripesPMI_Seq.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job secondJob = Job.getInstance(customConfiguration, "Loading Side Data and calculating PMI");
    secondJob.setJobName(StripesPMI_Seq.class.getSimpleName());
    secondJob.setJarByClass(StripesPMI_Seq.class);
    
    secondJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));
    
    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(HMapStIW.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(DoubleWritable.class);

    secondJob.setMapperClass(MyMapper.class);
    //secondJob.setCombinerClass(MyCombiner.class);
    secondJob.setReducerClass(MyReducer.class);
    secondJob.setOutputFormatClass(SequenceFileOutputFormat.class);


    Path outputDir = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputDir, true);

    long startTime2 = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI_Seq(), args);
  }

}
