package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import tl.lin.data.map.HMapStIW;
import tl.lin.data.pair.PairOfStrings;

public class StripesPMI_InMapper extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(StripesPMI_InMapper.class);
  private static double N = 0;

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

      Map<String, Integer> map = getMap();

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
      Mapper<LongWritable, Text, Text, HMapStIW> {

    private final static Text KEY = new Text();
    private Map<String, HMapStIW> map;
    private static final int FLUSH_SIZE = 2000000;

    @Override
    public void setup(Context context) {
      map = getMap();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      Map<String, HMapStIW> map = getMap();

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
        if (keyTerm.length() == 0) {
          continue;
        }
        for (int j = (i + 1); j < length; j++) {
          if (lineWords[j].length() == 0) {
            continue;
          }
            if (map.containsKey(keyTerm)) {
                if (map.get(keyTerm).containsKey(lineWords[j])) {
                    map.get(keyTerm).put(lineWords[j],
                                         map.get(keyTerm).get(lineWords[j]) + 1);
                } else {
                    map.get(keyTerm).put(lineWords[j], 1);
                }
            } else {
                HMapStIW newMap = new HMapStIW();
                newMap.put(lineWords[j], 1);
                map.put(keyTerm, newMap);
            }
        }
      }
      flush(context, false);
    } 

    public void flush(Context context, boolean forceFlush) throws IOException,
        InterruptedException {
      Map<String, HMapStIW> map = getMap();
      if (!forceFlush) {
        int size = map.size();
        if (size < FLUSH_SIZE) {
          return;
        }
      }
      // Flush as the size has exceeded
      for (String key : map.keySet()) {
        KEY.set(key);
        context.write(KEY, map.get(key));
      }
      // Now clear the map !
      map.clear();
    }

    public Map<String, HMapStIW> getMap() {
      if (null == map) {
        map = new HashMap<String, HMapStIW>();
      }
      return map;
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
      flush(context, true); // flush over here no matter what
      N = context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      Path path =
          new Path(
              "/user/hdedu6/lines");
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      FSDataOutputStream out = fs.create(path);
      out.writeDouble(N);
      out.close();
    }
  }


  private static class MyReducer extends
      Reducer<Text, HMapStIW, PairOfStrings, DoubleWritable> {

    private static final PairOfStrings PAIR_OF_WORDS = new PairOfStrings();
    private static final DoubleWritable PMI = new DoubleWritable();
    private static HashMap<String, Integer> dictionary =
        new HashMap<String, Integer>();

    @Override
    public void setup(Context context) throws IOException {

      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      Path path1 =
          new Path(
              "/user/hdedu6/lines");
      FSDataInputStream in = fs.open(path1);
      N = in.readDouble();

      System.out.println("Number of lines : " + N);
      /*
       * now we have to populate the dictionary to get the individual word
       * counts which is the output of the first mapper
       */
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);

      String path = System.getProperty("user.dir");
      Path pathToFile =
          new Path(
              "/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/stripesWordCount/part-r-00000");

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
        while ((line = bufferedReader.readLine()) != null) {
          String[] arr = line.split("\\s+");
          if (arr.length == 2) {
            dictionary.put(arr[0], Integer.parseInt(arr[1]));
          } else {
            LOG.info("Some error while creating dictionary.");
          }
        }
      } catch (Exception e) {
        LOG.info("Some error occured while reading the file");
      } finally {
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
          double pmi = Math.log10((numerator) / (prob_occ_x * prob_occ_y));
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
    String mapperOneOutputPath =
        "/user/hdedu6/Big_Data_Assignments/bigdata-assignments/assignment2/stripesWordCount";

    Path path2 = new Path(mapperOneOutputPath);

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(path2)) {
      fs.delete(new Path(mapperOneOutputPath), true);
    }

    int reduceTasks =
        cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + StripesPMI_InMapper.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + mapperOneOutputPath);
    LOG.info(" - num reducers: " + 1);

    Configuration customConfiguration = getConf();
    customConfiguration.set("mapOneOutput", mapperOneOutputPath);

    Job firstJob = Job.getInstance(customConfiguration);
    firstJob.setJobName("Sentence Wise Word Counter");
    firstJob.setJarByClass(StripesPMI_InMapper.class);

    firstJob.setNumReduceTasks(1); // using one reducer for the first job

    FileInputFormat.setInputPaths(firstJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(firstJob, new Path(mapperOneOutputPath));

    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);

    firstJob.setMapperClass(WordCountSentenceMapper.class);
    firstJob.setReducerClass(WordCountSentenceReducer.class);

    Path outputmapperonePath = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputmapperonePath, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    /* JOB ONE CONFIGURATION ENDS */

    /* JOB TWO CONFIGURATION STARTS */

    LOG.info("Tool name: " + StripesPMI_InMapper.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job secondJob =
        Job.getInstance(customConfiguration,
            "Loading Side Data and calculating PMI");
    secondJob.setJobName(StripesPMI_InMapper.class.getSimpleName());
    secondJob.setJarByClass(StripesPMI_InMapper.class);

    secondJob.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));

    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(HMapStIW.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(DoubleWritable.class);

    secondJob.setMapperClass(MyMapper.class);
    secondJob.setReducerClass(MyReducer.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);

    Path outputDir = new Path(outputPath);
    FileSystem.get(customConfiguration).delete(outputDir, true);

    long startTime2 = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI_InMapper(), args);
  }

}