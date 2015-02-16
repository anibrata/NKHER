package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Iterator;
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
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.nkher.HMapStIW;
import edu.umd.nkher.PairOfStrings;
import cern.colt.Arrays;

public class PairsPMI_Seq extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(PairsPMI_Seq.class);
  
  private static class WordCountSentenceMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    // Objects for reuse
    private final static Text KEY = new Text();
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString();
      StringTokenizer t = new StringTokenizer(line);
      Set<String> unique = new HashSet<String>();
      String token = "";

      while (t.hasMoreTokens()) {
        token = t.nextToken();

        if (unique.add(token)) {
          KEY.set(token);
          context.write(KEY, ONE);
        }
      }
    }
  }

  // First stage Reducer: Totals counts for each Token and Token Pair
  private static class WordCountSentenceReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects
    private final static IntWritable TOTAL = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      TOTAL.set(sum);
      context.write(key, TOTAL);
    }
  }

  private static class MyMapper extends
      Mapper<LongWritable, Text, PairOfStrings, DoubleWritable> {

    private final static PairOfStrings COOCCUR = new PairOfStrings();
    private final static DoubleWritable ONE = new DoubleWritable(1.0);

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
        for (int j = i + 1; j < length; j++) {
          COOCCUR.set(lineWords[i], lineWords[j]);
          context.write(COOCCUR, ONE);

        }
      }

    }
  }

  private static class MyCombiner extends
      Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> {
    private static DoubleWritable SUM = new DoubleWritable();

    @Override
    public void reduce(PairOfStrings pair, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      double sum = 0;
      Iterator<DoubleWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(pair, SUM);
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> {

	private static DoubleWritable PMI = new DoubleWritable();
	private static PairOfStrings PAIR = new PairOfStrings();
	private static double N = 114863.0; // got this from a standalone java file
	private static Map<String, Integer> dictionary =
		        new HashMap<String, Integer>();

    @Override
    public void setup(Context context) throws IOException {
      /*
       * Here we will try to put the word counts from the previous run into the dictionary
       */
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      
      String path = System.getProperty("user.dir");
      Path pathToFile = new Path("/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/pairsWordCount/part-r-00000");

      BufferedReader bufferedReader = null;
      FSDataInputStream fsdis = null;
      InputStreamReader isr = null;
      try {
        fsdis = fileSystem.open(pathToFile);
        isr = new InputStreamReader(fsdis);
        bufferedReader = new BufferedReader(isr);
      } catch (Exception e) {
    	  e.printStackTrace();
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
        bufferedReader.close();
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {

      double number_of_occ = 0.0;
      Iterator<DoubleWritable> iter = values.iterator();
      while (iter.hasNext()) {
    	  number_of_occ += iter.next().get();
      }
      
      if (number_of_occ >= 10.0) {
    	  	String word1 = key.getLeftElement();
    	  	String word2 = key.getRightElement();
    	  	double p_x_y = number_of_occ / N;
            double p_x =  (double)dictionary.get(word1) / N;
            double p_y =  (double)dictionary.get(word2) / N;
            double pmi =  (double)Math.log10(p_x_y / (p_x * p_y));
            PMI.set(pmi);
            context.write(key, PMI);
      }
    }
  }
  
  public PairsPMI_Seq() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  @SuppressWarnings("static-access")
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
    String outputPath = cmdline.getOptionValue(OUTPUT);  // this output will be used by second map reduce job
    String path = System.getProperty("user.dir");
    String mapperOneOutputPath = "/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/pairsWordCount";
    
    Path path2 = new Path(mapperOneOutputPath);
    
    FileSystem fs = FileSystem.get(getConf());
    if(fs.exists(path2)) {
    	fs.delete(new Path(mapperOneOutputPath), true);
    }

    int reduceTasks =
        cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + PairsPMI_Seq.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + mapperOneOutputPath);
    LOG.info(" - number of reducers: " + 1);

    Configuration customConfiguration = getConf();
    customConfiguration.set("mapOneOutput", mapperOneOutputPath);

    Job firstJob = Job.getInstance(customConfiguration);
    firstJob.setJobName(PairsPMI_Seq.class.getSimpleName());
    firstJob.setJarByClass(PairsPMI_Seq.class);

    firstJob.setNumReduceTasks(1); // using one reducer for the first job

    FileInputFormat.setInputPaths(firstJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(firstJob, new Path(mapperOneOutputPath));

    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);

    firstJob.setMapperClass(WordCountSentenceMapper.class);
    //firstJob.setCombinerClass(WordCountSentenceReducer.class);
    firstJob.setReducerClass(WordCountSentenceReducer.class);

    Path firstMapperOutputDir = new Path(mapperOneOutputPath);
    FileSystem.get(customConfiguration).delete(firstMapperOutputDir, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
        / 1000.0 + " seconds");

    /* JOB ONE CONFIGURATION ENDS */

    /* JOB TWO CONFIGURATION STARTS */
    
    LOG.info("Tool: " + PairsPMI_Seq.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    //Job secondJob = Job.getInstance(customConfiguration);
    Job secondJob = new Job(customConfiguration, "Loading Side Data and calculating PMI");
    secondJob.setJobName(PairsPMI_Seq.class.getSimpleName());
    secondJob.setJarByClass(PairsPMI_Seq.class);

    secondJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));
    //TextOutputFormat.setOutputPath(secondJob, new Path(outputPath));
    
    //secondJob.setMapOutputKeyClass(Text.class);
    //secondJob.setMapOutputValueClass(IntWritable.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(DoubleWritable.class);
    
    secondJob.setMapperClass(MyMapper.class);
    //secondJob.setCombinerClass(MyCombiner.class);
    secondJob.setReducerClass(MyReducer.class);
    secondJob.setOutputFormatClass(SequenceFileOutputFormat.class);

    Path outputDir = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
        / 1000.0 + " seconds");

    return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI_Seq(), args);
  }
}