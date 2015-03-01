package edu.umd.nkher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;

public class BuildInvertedIndexCompressed extends Configured implements Tool {

  private static final Logger LOG = Logger
      .getLogger(BuildInvertedIndexCompressed.class);

  protected static class InvertedIndexCompressedMapper extends
      Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {

    private static final PairOfStringInt KEYPAIR = new PairOfStringInt();
    private static final VIntWritable VALUE = new VIntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable documentNo, Text value, Context context)
        throws IOException, InterruptedException {
      COUNTS.clear();
      String line = value.toString();
      String splittedDoc[] = line.split("\\s+");
      for (String token : splittedDoc) {
        if (token == null || token.length() == 0) {
          continue;
        }
        COUNTS.increment(token);
      }
      for (PairOfObjectInt<String> pair : COUNTS) {
        KEYPAIR.set(pair.getLeftElement(), (int) documentNo.get());
        VALUE.set(pair.getRightElement());
        context.write(KEYPAIR, VALUE);

        // Following order of inversion, for getting the document frequency first
        KEYPAIR.set(pair.getLeftElement(), '*');
        VALUE.set(1);
        context.write(KEYPAIR, VALUE);
      }
    }
  }

  protected static class InvertedIndexCompressedReducer
      extends
      Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable> {

    private static int docFreq;
    private int termFreq;
    private int currdocNumber;
    private int prevdocNumber = 0;
    private int dGap = 0;
    private static String prevTerm;
    private String currentTerm;
    private final static Text term = new Text();
    private static ByteArrayOutputStream byteOpStream;
    private static DataOutputStream dataOpStream;
    private static Map<String, Integer> docFrequencyMap;

    @Override
    public void setup(Context context) {
      docFreq = 0;
      prevTerm = null;
      currentTerm = null;
      byteOpStream = new ByteArrayOutputStream();
      dataOpStream = new DataOutputStream(byteOpStream);
      docFrequencyMap = new HashMap<String, Integer>();
    }

    @Override
    public void reduce(PairOfStringInt key, Iterable<VIntWritable> values,
        Context context) throws IOException, InterruptedException {

      Iterator<VIntWritable> iter = values.iterator();

      /*
       * Calculating the document frequency first using the order of inversion
       * technique and storing it in the HashMap
       */
      if (key.getRightElement() == '*') {
        docFreq = 0;
        while (iter.hasNext()) {
          docFreq += iter.next().get();
        }
        docFrequencyMap.put(key.getLeftElement(), docFreq);
      }
      /*
       * Calculate term frequency, document gaps and store them in the byte out
       * put stream.
       */
      else {
        currentTerm = key.getLeftElement();
        currdocNumber = key.getRightElement();

        termFreq = 0;
        // calculate the term frequency
        while (iter.hasNext()) {
          termFreq += iter.next().get();
        }

        if (prevTerm == null || !currentTerm.equals(prevTerm)) {
          // writing the buffered data to the stream
          if (prevTerm != null) {
            ByteArrayOutputStream finalByteStream = new ByteArrayOutputStream();
            DataOutputStream finalDataOpStream = new DataOutputStream(finalByteStream);
            /* Writing the document frequency for the term first */
            WritableUtils.writeVInt(finalDataOpStream, docFrequencyMap.get(prevTerm));
            /* Writing the postings for the term */
            finalDataOpStream.write(byteOpStream.toByteArray());

            if (prevTerm.equals("&c") || prevTerm.equals("''lo")) {
              // get the document frequency
              System.out.println("Term is : " + prevTerm);
              System.out.println("Size of byte output stream is "
                  + finalByteStream.size());
              ByteArrayInputStream bytein =
                  new ByteArrayInputStream(finalByteStream.toByteArray());
              DataInputStream in = new DataInputStream(bytein);
              while (in.available() > 0) {
                int data = WritableUtils.readVInt(in);
                System.out.println("Data is : " + data);
              }
              bytein.close();
              in.close();
            }
            
            term.set(prevTerm);
            context.write(term, new BytesWritable(finalByteStream.toByteArray()));

          }

          /*
           * Start writing postings for next term in the new stream which is
           * created in the document frequency
           */
          byteOpStream.flush();
          dataOpStream.flush();
          byteOpStream = new ByteArrayOutputStream();
          dataOpStream = new DataOutputStream(byteOpStream);
          
          prevdocNumber = 0;
          dGap = currdocNumber - prevdocNumber;
          WritableUtils.writeVInt(dataOpStream, dGap);
          WritableUtils.writeVInt(dataOpStream, termFreq);

        } else if (currentTerm.equals(prevTerm)) {
          dGap = currdocNumber - prevdocNumber;
          WritableUtils.writeVInt(dataOpStream, dGap);
          WritableUtils.writeVInt(dataOpStream, termFreq);
        }
        prevdocNumber = currdocNumber;
        prevTerm = currentTerm;
      }
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
      ByteArrayOutputStream finalByteStream = new ByteArrayOutputStream();
      DataOutputStream finalDataOpStream = new DataOutputStream(finalByteStream);
      finalDataOpStream.write(byteOpStream.toByteArray());
      term.set(currentTerm);
      context.write(term, new BytesWritable(finalByteStream.toByteArray()));
      dataOpStream.close();
      byteOpStream.close();
    }
  }

  protected static class InvertedIndexCompressedPartitioner extends
      Partitioner<PairOfStringInt, VIntWritable> {
    @Override
    public int getPartition(PairOfStringInt pair, VIntWritable value,
        int numReduceTasks) {
      return (pair.getLeftElement().hashCode() & Integer.MAX_VALUE)
          % numReduceTasks;
    }
  }


  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

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

    LOG.info("Tool name: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(VIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(InvertedIndexCompressedMapper.class);
    job.setReducerClass(InvertedIndexCompressedReducer.class);
    job.setPartitionerClass(InvertedIndexCompressedPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
