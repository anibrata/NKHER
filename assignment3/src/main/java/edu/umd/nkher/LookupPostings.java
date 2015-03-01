package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;
import cern.colt.Arrays;

public class LookupPostings extends Configured implements Tool {

  private static final String INDEX = "index";
  private static final String COLLECTION = "collection";

  @Override
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INDEX));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(COLLECTION));

    org.apache.commons.cli.CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(COLLECTION)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(LookupPostings.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX);
    String collectionPath = cmdline.getOptionValue(COLLECTION);

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    Path path =
        new Path(
            "/Users/nameshkher/Documents/Hadoop-WorkSpace/InvertedIndex/invertedindex/part-r-00000");

    Text textKey = new Text(); // text to be found

    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
        new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();

    MapFile.Reader mapReader = new MapFile.Reader(path, config);
    FSDataInputStream collection = fs.open(new Path(collectionPath));
    BufferedReader bReader =
        new BufferedReader(new InputStreamReader(collection));

    System.out.println("Looking up postings for term starcross'd");
    textKey.set("starcross'd");
    mapReader.get(textKey, value); // calling the get function
    ArrayListWritable<PairOfInts> postings = value.getRightElement();
    for (PairOfInts pair : postings) {
      System.out.println(pair);
      collection.seek(pair.getLeftElement());
      System.out.println(bReader.readLine());
    }

    // Answer 2
    textKey.set("gold");
    mapReader.get(textKey, value);
    postings = value.getRightElement();
    HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    for (PairOfInts pair : postings) {
      if (!map.containsKey(pair.getRightElement())) {
        map.put(pair.getRightElement(), 1);
      } else {
        map.put(pair.getRightElement(), map.get(pair.getRightElement()) + 1);
      }
    }

    System.out.println("Histogram for Gold's tf values : ");
    for (Integer i : map.keySet()) {
      System.out.println("Tf -> " + i + ", Number of documents -> "
          + map.get(i));
    }

    // Answer 3.a
    textKey.set("silver");
    mapReader.get(textKey, value);
    postings = value.getRightElement();
    map = new HashMap<Integer, Integer>();
    for (PairOfInts pair : postings) {
      if (!map.containsKey(pair.getRightElement())) {
        map.put(pair.getRightElement(), 1);
      } else {
        map.put(pair.getRightElement(), map.get(pair.getRightElement()) + 1);
      }
    }

    System.out.println("Histogram for silver tf values : ");
    for (Integer i : map.keySet()) {
      System.out.println("Tf -> " + i + ", Number of documents -> "
          + map.get(i));
    }

    // Answer 3.b
    textKey.set("bronze");
    mapReader.get(textKey, value);
    postings = value.getRightElement();
    map = new HashMap<Integer, Integer>();
    for (PairOfInts pair : postings) {
      if (!map.containsKey(pair.getRightElement())) {
        map.put(pair.getRightElement(), 1);
      } else {
        map.put(pair.getRightElement(), map.get(pair.getRightElement()) + 1);
      }
    }

    System.out.println("Histogram for bronze tf values : ");
    for (Integer i : map.keySet()) {
      System.out.println("Tf -> " + i + ", Number of documents -> "
          + map.get(i));
    }

    collection.close();
    mapReader.close();

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LookupPostings(), args);
  }

}
