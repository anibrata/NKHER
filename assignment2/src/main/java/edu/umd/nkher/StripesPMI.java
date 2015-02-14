package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.nkher.HMapStIW;
import edu.umd.nkher.PairOfStrings;

public class StripesPMI extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  
  protected static class WordCountSentenceMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Text WORD = new Text();
    private final static IntWritable ONE = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      HashSet<String> hashSet = new HashSet<String>();
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        if (!hashSet.contains(token)) {
          hashSet.add(token);
          WORD.set(token);
          context.write(WORD, ONE);
        }
      }
    }
  }

  protected static final class WordCountSentenceReducer extends
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

  protected static class StripesPMI_Mapper extends
      Mapper<LongWritable, Text, Text, HMapStIW> {
    private static HMapStIW hashMap = new HMapStIW();
    private static Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      // StringTokenizer tokenizer = new StringTokenizer(line);

      String[] terms = line.split("\\s+");
      int len = terms.length;
      for (int i=0;i<len;i++) {
    	  String keyTerm = terms[i];
    	  if(keyTerm.length() == 0)
    		  continue;
    	  hashMap.clear();
    	  for(int j=0;j<len;j++) {
    		  if (j == i) 
    			  continue;
    		  if(terms[j].length() == 0)
    			  continue;
    		  hashMap.increment(terms[j]);
    	  }
    	  KEY.set(keyTerm);
    	  context.write(KEY, hashMap);
      }
      
//      ArrayList<String> lineWords = new ArrayList<String>();
//      while (tokenizer.hasMoreTokens()) {
//        lineWords.add(tokenizer.nextToken());
//      }

//      for (int i = 0; i < lineWords.size(); i++) {
//        String curWord = lineWords.get(i);
//        KEY.set(curWord); // setting the key for emiting
//        // now putting all the other words
//        for (int j = i + 1; j < lineWords.size(); j++) {
//          String word = lineWords.get(j);
//          if (!hashMap.containsKey(word)) {
//            hashMap.put(word, 1);
//          }
//        }
//        // now putting each word in the associative array
//        context.write(KEY, hashMap);
//        // clearing the map
//        hashMap.clear();
//      }
    }
  }

  protected static class StripesPMI_Combiner extends
      Reducer<Text, HMapStIW, Text, HMapStIW> {

    private static HMapStIW hashMap = new HMapStIW();

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      for (HMapStIW hmap : values) {
        for (String word : hmap.keySet()) {
          int curCount = hmap.get(word);
          if (hashMap.containsKey(word)) {
            hashMap.put(word, hashMap.get(word) + curCount);
          } else {
            hashMap.put(word, curCount);
          }
        }
      }
      context.write(key, hashMap);
    }
  }

  protected static class StripesPMI_Reducer extends
      Reducer<Text, HMapStIW, PairOfStrings, DoubleWritable> {

    private static final HashMap<String, Integer> hashMap =
        new HashMap<String, Integer>();
    private static final PairOfStrings PAIR_OF_WORDS = new PairOfStrings();
    private static final DoubleWritable PMI = new DoubleWritable();
    private static final double N = 156215.0; // number of sentences
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
      Path pathToFile = new Path(path + "/part-r-00000");

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
        String line = bufferedReader.readLine();
        while ( (line = bufferedReader.readLine()) != null ) {
          String[] arr = line.split(" ");
          if (arr.length == 2) {
            dictionary.put(arr[0], Integer.parseInt(arr[1]));
          } else {
            LOG.info("Some error while creating dictionary.");
          }
        }
      }
 catch (Exception e) {
        LOG.info("Some error occured while reading the file");
      } finally {
        bufferedReader.close();
        fsdis.close();
        isr.close();
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {

      for (HMapStIW hmap : values) {
        for (String word : hmap.keySet()) {
          int curCount = hmap.get(word);
          if (hashMap.containsKey(word)) {
            hashMap.put(word, hashMap.get(word) + curCount);
          } else {
            hashMap.put(word, curCount);
          }
        }
      }
      
      String leftWordOfPair = key.toString();
      
      for (String rightWordOfPair : hashMap.keySet()) {
        double p_x_y = hashMap.get(rightWordOfPair);
        if (p_x_y >= 10) {
          PAIR_OF_WORDS.set(leftWordOfPair, rightWordOfPair);
          double numerator = p_x_y / N;
          double prob_occ_x = dictionary.get(leftWordOfPair) / N;
          double prob_occ_y = dictionary.get(rightWordOfPair) / N;
          double pmi = Math.log((numerator) / (prob_occ_x * prob_occ_y));
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
    String map1OutputPath = path;

    int reduceTasks =
        cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + map1OutputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Configuration configuration = getConf();
    configuration.set("firstMapperOutPut", map1OutputPath);

    Job firstJob = Job.getInstance(configuration);
    firstJob.setJobName("Sentence Wise Word Counter");
    firstJob.setJarByClass(StripesPMI.class);
    firstJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(firstJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(firstJob, new Path(map1OutputPath));

    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);

    firstJob.setMapperClass(WordCountSentenceMapper.class);
    firstJob.setReducerClass(WordCountSentenceReducer.class);

    Path outputmapperonePath = new Path(map1OutputPath);
    FileSystem.get(configuration).delete(outputmapperonePath, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    /* JOB ONE CONFIGURATION ENDS */

    /* JOB TWO CONFIGURATION STARTS */

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job secondJob = Job.getInstance(configuration);
    secondJob.setJobName(StripesPMI.class.getSimpleName());
    secondJob.setJarByClass(StripesPMI.class);
    secondJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));

    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(HMapStIW.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(DoubleWritable.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);

    secondJob.setMapperClass(StripesPMI_Mapper.class);
    secondJob.setCombinerClass(StripesPMI_Combiner.class);
    secondJob.setReducerClass(StripesPMI_Reducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(configuration).delete(outputDir, true);

    long startTime2 = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }

}
