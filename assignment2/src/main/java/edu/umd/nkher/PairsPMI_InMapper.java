package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfStrings;
import cern.colt.Arrays;

public class PairsPMI_InMapper extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(PairsPMI_InMapper.class);
  private static long numberOfLines = 0;

  private static class WordCountSentenceMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    private Map<String, Integer> map;
    private final static Text KEY = new Text();
    private final static IntWritable INT = new IntWritable();
    private static final int FLUSH_SIZE = 200000;

    @Override
    public void setup(Context context) {
      map = getMap();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      // Map<String, Integer> map = getMap();

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        if (map.containsKey(token)) {
          int total = map.get(token).intValue() + 1;
          map.put(token, total);
        } else {
          map.put(token, 1);
        }
      }
      // this will flush if required
      flush(context, false);
    }

    public void flush(Context context, boolean forceFlush) throws IOException,
        InterruptedException {
      Map<String, Integer> map = getMap();
      if (!forceFlush) {
        int size = map.size();
        if (size < FLUSH_SIZE) {
          return;
        }
      }
      // else write it to context
      for (String key : map.keySet()) {
        int total = map.get(key).intValue();
        INT.set(total);
        KEY.set(key);
        context.write(KEY, INT);
      }
      // now empty the map !
      map.clear();
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      // flush no matter what as we have to remove the items !
      flush(context, true);
    }

    public Map<String, Integer> getMap() {
      if (null == map) {
        map = new HashMap<String, Integer>();
      }
      return map;
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

    private final static DoubleWritable DOUBLE = new DoubleWritable();
    private Map<PairOfStrings, Double> map;
    private static final int FLUSH_SIZE = 2000000;

    @Override
    public void setup(Context context) {
      // This will initialize the map
      map = getMap();
    }

      @Override
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
      
      Map<PairOfStrings, Double> map = getMap();

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
          PairOfStrings PAIR = new PairOfStrings(lineWords[i], lineWords[j]);
          // add to the map
          if (map.containsKey(PAIR)) {
            double total = map.get(PAIR).doubleValue() + 1.0;
            map.put(PAIR, total);
          } else {
            map.put(PAIR, 1.0);
          }
        }
      }
      flush(context, false);
    }

    public void flush(Context context, boolean forceFlush) throws IOException,
        InterruptedException {
      // getting the map to flush
      Map<PairOfStrings, Double> map = getMap();
      if (!forceFlush) {
        int size = map.size();
        if (size < FLUSH_SIZE) {
          return;
        }
      }
      // continue and write to context if reached flush_size
      System.out.println("Flushing out map of " + map.size() + " elements.");
      for (PairOfStrings pos : map.keySet()) {
        DOUBLE.set(map.get(pos));
        context.write(pos, DOUBLE);
      }
      // clear the map !
      map.clear();
    }

    public Map<PairOfStrings, Double> getMap() {
      if (null == map) {
        map = new HashMap<PairOfStrings, Double>();
      }
      return map;
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      long lines =
 context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
      numberOfLines += lines;
      flush(context, true); // has to flush in any case
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> {

    private static DoubleWritable PMI = new DoubleWritable();
    private static final double N = numberOfLines; // number of sentences
    private static Map<String, Integer> dictionary =
        new HashMap<String, Integer>();

    @Override
    public void setup(Context context) throws IOException {
      System.out.println("Number of lines : " + N);
      /*
       * Here we will try to put the word counts from the previous run into the
       * dictionary
       */
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);

      String path = System.getProperty("user.dir");
      Path pathToFile =
          new Path(
              "/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/pairsWordCount/part-r-00000");

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
        while ((line = bufferedReader.readLine()) != null) {
          String[] arr = line.split("\\s+");
          if (arr.length == 2) {
            dictionary.put(arr[0], Integer.parseInt(arr[1]));
          } else {
            System.out.println("Some error while creating dictionary.");
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
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
        double p_x = (double) dictionary.get(word1) / N;
        double p_y = (double) dictionary.get(word2) / N;
        double pmi = Math.log10(p_x_y / (p_x * p_y));
        PMI.set(pmi);
        context.write(key, PMI);
      }
    }
  }

  public PairsPMI_InMapper() {
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
    String outputPath = cmdline.getOptionValue(OUTPUT); // this output will be
                                                        // used by second map
                                                        // reduce job
    String path = System.getProperty("user.dir");
    String mapperOneOutputPath =
        "/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/pairsWordCount";

    Path path2 = new Path(mapperOneOutputPath);

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(path2)) {
      fs.delete(new Path(mapperOneOutputPath), true);
    }

    int reduceTasks =
        cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + PairsPMI_InMapper.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + mapperOneOutputPath);
    LOG.info(" - number of reducers: " + 1);

    Configuration customConfiguration = getConf();
    customConfiguration.set("mapOneOutput", mapperOneOutputPath);

    Job firstJob = Job.getInstance(customConfiguration);
    firstJob.setJobName(PairsPMI_InMapper.class.getSimpleName());
    firstJob.setJarByClass(PairsPMI_InMapper.class);

    firstJob.setNumReduceTasks(1); // using one reducer for the first job

    FileInputFormat.setInputPaths(firstJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(firstJob, new Path(mapperOneOutputPath));

    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);

    firstJob.setMapperClass(WordCountSentenceMapper.class);
    firstJob.setReducerClass(WordCountSentenceReducer.class);

    Path firstMapperOutputDir = new Path(mapperOneOutputPath);
    FileSystem.get(customConfiguration).delete(firstMapperOutputDir, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
        / 1000.0 + " seconds");

    /* JOB ONE CONFIGURATION ENDS */

    /* JOB TWO CONFIGURATION STARTS */

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    // Job secondJob = Job.getInstance(customConfiguration);
    Job secondJob =
        new Job(customConfiguration, "Loading Side Data and calculating PMI");
    secondJob.setJobName(PairsPMI.class.getSimpleName());
    secondJob.setJarByClass(PairsPMI.class);

    secondJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));
    // TextOutputFormat.setOutputPath(secondJob, new Path(outputPath));

    // secondJob.setMapOutputKeyClass(Text.class);
    // secondJob.setMapOutputValueClass(IntWritable.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(DoubleWritable.class);

    secondJob.setMapperClass(MyMapper.class);
    secondJob.setReducerClass(MyReducer.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);

    Path outputDir = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
        / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI_InMapper(), args);
  }
}

      
