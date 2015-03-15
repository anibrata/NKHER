package edu.umd.nkher;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;
import edu.umd.nkher.PageRankNodeEnhanced;

public class BuildPersonalizedPageRankRecords extends Configured implements Tool {

  Logger LOG = Logger.getLogger(BuildPersonalizedPageRankRecords.class);
  private static final String NODE_CNT_FIELD = "node.cnt";
  private static final String NUMBER_OF_SOURCES = "numberOfSources";
  private static final String SOURCE_VALUES = "sourceValues";

  private static class MyMapper extends
      Mapper<LongWritable, Text, IntWritable, PageRankNodeEnhanced> {

    private static final IntWritable nid = new IntWritable();
    private static final PageRankNodeEnhanced node = new PageRankNodeEnhanced();
    private static final HashMap<Integer, Integer> sourcesMap = new HashMap<Integer, Integer>();
    int numberOfSources;

    @Override
    public void setup(
        Mapper<LongWritable, Text, IntWritable, PageRankNodeEnhanced>.Context context) {
      numberOfSources = context.getConfiguration().getInt(NUMBER_OF_SOURCES, 0);
      if (numberOfSources == 0) {
        throw new RuntimeException(NUMBER_OF_SOURCES
            + " cannot be 0!. No sources provided");
      }
      node.setType(PageRankNodeEnhanced.Type.Complete);
      // Parsing the sources and putting in a set
      String line[] = context.getConfiguration().get(SOURCE_VALUES).split(",");
      for (int i = 0; i < line.length; i++) {
        System.out.println("Source : " + i + " is : " + line[i]);
        sourcesMap.put(Integer.parseInt(line[i]), i);
      }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String arr[] = value.toString().trim().split("\\s+");

      int currNode = Integer.parseInt(arr[0]);
      ArrayListOfFloatsWritable pagerankList = new ArrayListOfFloatsWritable();

      /* If Source node and update its starting PageRank value to 1 else 0 */
      
      if (sourcesMap.containsKey(currNode)) { 
          int index = sourcesMap.get(currNode); 
          float[] pr = new float[numberOfSources]; 
          for (int i = 0; i < numberOfSources; i++) { 
            if (i == index) { 
              pr[i] = (float) StrictMath.log(1.0f); } 
            
            else { pr[i] = (float)
                  StrictMath.log(0.0f); 
              } 
          } 
          pagerankList = new ArrayListOfFloatsWritable(pr); 
          node.setPageRank(pagerankList); 
       } else {
            float[] pr = new float[numberOfSources]; 
            for (int i = 0; i < numberOfSources; i++) {
              pr[i] = (float) StrictMath.log(0.0f); 
              } 
        pagerankList = new ArrayListOfFloatsWritable(pr);
        node.setPageRank(pagerankList);
       }
       

      nid.set(currNode); // setting the key

      /* Setting the Adjacency lists */
      if (arr.length == 1) {
        node.setNodeId(currNode);
        node.setAdjacencyList(new ArrayListOfIntsWritable());
      }
      else { // means it is not a dangling node
        node.setNodeId(currNode);
        int[] neighbours = new int[arr.length - 1];
        for (int i = 1; i < arr.length; i++) {
          neighbours[i - 1] = Integer.parseInt(arr[i]);
        }
        node.setAdjacencyList(new ArrayListOfIntsWritable(neighbours));
      }

      context.getCounter("graph", "numNodes").increment(1);
      context.getCounter("graph", "numEdges").increment(arr.length - 1);

      if (arr.length > 1) {
        context.getCounter("graph", "numActiveNodes").increment(1);
      }

      context.write(nid, node);
    }
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_NODES = "numNodes";
  private static final String SOURCES = "sources";
  
  @Override
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)
        || !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    // Start Job related configuration
    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    String sourceValues = cmdline.getOptionValue(SOURCES);
    String sources[] = sourceValues.split(",");

    LOG.info("Tool name: " + BuildPersonalizedPageRankRecords.class.getSimpleName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    LOG.info(" - numNodes: " + n);
    StringBuilder displaySources = new StringBuilder();
    for (int i = 0; i < sources.length; i++) {
      displaySources.append(sources[i] + " ");
    }
    LOG.info(" - sources are : " + displaySources);

    /* STARTING JOB CONFIGURATION */

    Configuration conf = getConf();
    conf.setInt(NODE_CNT_FIELD, n);
    conf.setInt(NUMBER_OF_SOURCES, sources.length);
    conf.set(SOURCE_VALUES, sourceValues);

    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    Job job = Job.getInstance(conf);
    job.setJobName(BuildPersonalizedPageRankRecords.class.getSimpleName()
        + ": "
        + inputPath);
    job.setJarByClass(BuildPersonalizedPageRankRecords.class);

    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNodeEnhanced.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNodeEnhanced.class);

    job.setMapperClass(MyMapper.class);

    // delete o/p directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    /* ENDING JOB CONFIGURATION */
   
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildPersonalizedPageRankRecords(), args);
  }
}
