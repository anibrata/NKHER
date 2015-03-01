package edu.umd.nkher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class BuildInvertedIndex extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndex.class);

  protected static class MyMapper
      extends
      Mapper<LongWritable, Text, Text, PairOfInts> {

    private final static Text KEY = new Text();
    private final static PairOfInts POI = new PairOfInts();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      HashMap<String, Integer> hmap = new HashMap<String, Integer>();
      String word = "";
      // iterating over the word list and storing in the HashMap
      while (tokenizer.hasMoreTokens()) {
        word = tokenizer.nextToken();
        if (hmap.containsKey(word)) {
          hmap.put(word, hmap.get(word) + 1);
        } else {
          hmap.put(word, 1);
        }
      }
      // now emitting all the terms
      for (String words : hmap.keySet()) {
        KEY.set(words);
        POI.set((int) key.get(), hmap.get(words));
        context.write(KEY, POI);
      }
    }
  }

  protected static class MyReducer extends
      Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {

    private static final IntWritable DF = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values,
        Context context) throws IOException, InterruptedException {

      Iterator<PairOfInts> iter =
          values.iterator();

      ArrayListWritable<PairOfInts> postings =
          new ArrayListWritable<PairOfInts>();

      while (iter.hasNext()) {
        PairOfInts p = iter.next().clone();
        if (key.toString().equals("'even")) {
          System.out.println("Byte code is : " + p.getLeftElement());
        }
        postings.add(p);
      }
      
      Collections.sort(postings);
      
      // int n = postings.size();
      // for (int i = 1; i < n - 1; i++) {
      // for (int j = 0; j <= n - i - 1; j++) {
      // if (postings.get(j).getLeftElement().get() > postings.get(j + 1)
      // .getLeftElement().get()) {
      // // swap here
      // PairOfWritables<IntWritable, IntWritable> temp =
      // new PairOfWritables<IntWritable, IntWritable>();
      // temp = postings.get(j + 1);
      // postings.set(j + 1, postings.get(j));
      // postings.set(j, temp);
      // }
      // }
      // }

      DF.set(postings.size());
      context.write(key, 
          new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF,
              postings));
    }
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @Override
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

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);

    int reduceTasks =
        cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + BuildInvertedIndex.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndex.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndex.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfInts.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    // job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndex(), args);
  }

}
