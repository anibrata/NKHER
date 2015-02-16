/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package edu.umd.nkher;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import edu.umd.nkher.PairOfStrings;

public class PairsPMI_2 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI_2.class);

  protected static class MyMapper extends
      Mapper<LongWritable, Text, PairOfStrings, DoubleWritable> {
    private static final DoubleWritable ONE = new DoubleWritable(1);
    private static PairOfStrings PAIR = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      Set<String> sortedTokens = new TreeSet<String>();
      while (tokenizer.hasMoreTokens()) {
        sortedTokens.add(tokenizer.nextToken());
      }
     
      // 1. First writing the single words and their count per sentence to the
      // context
      for (String s : sortedTokens) {
        PAIR.set("", s);
        context.write(PAIR, ONE);
      }

      // 2. Secondly writing the co-occcurences to the context
      String[] lineWords = new String[sortedTokens.size()];
      sortedTokens.toArray(lineWords); // filling the array
      int len = lineWords.length;
      for (int i = 0; i < len; i++) {
        for (int j = (i + 1); j < len; j++) {
          PAIR.set(lineWords[i], lineWords[j]);
          context.write(PAIR, ONE);
        }
      }

    }
  }

  protected static class MyCombiner extends
      Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> {
    private static final DoubleWritable SUM = new DoubleWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      double sum = 0;
      Iterator<DoubleWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  protected static class MyReducer extends
      Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> {
    private static final DoubleWritable FINAL_COUNT = new DoubleWritable();
    private static DoubleWritable PMI = new DoubleWritable();
    private static double N = 156215.0; // calculated in a separate java file

    // hashmap to store the words
    private static HashMap<String, Double> dictionary =
        new HashMap<String, Double>();

    @Override
    public void reduce(PairOfStrings key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      double sum = 0;
      Iterator<DoubleWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      FINAL_COUNT.set(sum);

      if (key.getLeftElement().equals("")) {
        dictionary.put(key.getRightElement(), FINAL_COUNT.get());
      }
      else { // Calculating PMI
        if (sum >= 10) { // checking for atleast ten or more lines
          double pmi;
          double numberator = sum / N;
          double prob_occ_x = dictionary.get(key.getLeftElement()) / N;
          double prob_occ_y = dictionary.get(key.getRightElement()) / N;
          pmi = Math.log10( (numberator)  / (prob_occ_x * prob_occ_y) );
          PMI.set(pmi);
          context.write(key, PMI);
        }
      }
    }
  }

  private PairsPMI_2() {
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

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks =
        cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + PairsPMI_2.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(PairsPMI_2.class.getSimpleName());
    job.setJarByClass(PairsPMI_2.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
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
    ToolRunner.run(new PairsPMI_2(), args);
  }
}