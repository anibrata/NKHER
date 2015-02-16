package edu.umd.nkher;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;

import tl.lin.*;
import com.google.common.collect.Lists;

public class AnalyzePMI {
  
  public static List<PairOfWritables<PairOfStrings, DoubleWritable>> bubblesort(
      List<PairOfWritables<PairOfStrings, DoubleWritable>> list) {
    // sorting the list through bubble sort in descending manner
    int n = list.size();
    for (int i = 1; i < n - 1; i++) {
      for (int j = 0; j <= n - i - 1; j++) {
        if (list.get(j).getRightElement().get() < list.get(j + 1)
            .getRightElement().get()) {
          // swap here
          PairOfWritables<PairOfStrings, DoubleWritable> temp = list.get(j + 1);
          list.set(j + 1, list.get(j));
          list.set(j, temp);
        }
      }
    }
    return list;
  }

  private static final String INPUT = "input";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) throws IOException {
      Options options = new Options();

      options.addOption(OptionBuilder.withArgName("path").hasArg()
          .withDescription("input path").create(INPUT));

      CommandLine cmdline = null;
      CommandLineParser parser = new GnuParser();

      try {
        cmdline = parser.parse(options, args);
      } catch (ParseException exp) {
        System.err.println("Error parsing command line: " + exp.getMessage());
        System.exit(-1);
      }

      if (!cmdline.hasOption(INPUT)) {
        System.out.println("args: " + Arrays.toString(args));
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
      formatter.printHelp(AnalyzePMI.class.getName(), options);
        ToolRunner.printGenericCommandUsage(System.out);
        System.exit(-1);
      }

      String inputPath = cmdline.getOptionValue(INPUT);
      System.out.println("input path: " + inputPath);

    List<PairOfWritables<PairOfStrings, DoubleWritable>> pairs =
        Lists.newArrayList();

    pairs = SequenceFileUtils.readDirectory(new Path(inputPath));

    // To get the highest pair
    double highestPmi = Double.MIN_VALUE;
    PairOfWritables<PairOfStrings, DoubleWritable> highestP = pairs.get(0);
    for (int i = 1; i < pairs.size(); i++) {
      PairOfWritables<PairOfStrings, DoubleWritable> current = pairs.get(i);
      if (current.getRightElement().get() > highestPmi) {
        highestPmi = current.getRightElement().get();
        highestP = current;
      } else if (current.getRightElement().get() == highestPmi) {
        if (current.getLeftElement().compareTo(highestP.getLeftElement()) > 0) {
          highestPmi = current.getRightElement().get();
          highestP = current;
        }
      }
    }

    System.out.println("Pair with highest PMI value is "
        + highestP.getLeftElement() + " and its PMI value is "
        + highestP.getRightElement());


    List<PairOfWritables<PairOfStrings, DoubleWritable>> cloudList =
        Lists.newArrayList();
    List<PairOfWritables<PairOfStrings, DoubleWritable>> loveList =
        Lists.newArrayList();

    Set<DoubleWritable> distinctPMI = new HashSet<DoubleWritable>();

    for (PairOfWritables<PairOfStrings, DoubleWritable> p : pairs) {
      PairOfStrings cooccurences = p.getLeftElement();
      if (cooccurences.getLeftElement().equals("cloud")
          || cooccurences.getRightElement().equals("cloud")) {
        cloudList.add(p);
      }
      if (cooccurences.getLeftElement().equals("love")
          || cooccurences.getRightElement().equals("love")) {
        loveList.add(p);
      }
      if(!distinctPMI.contains(p.getRightElement())) {
        distinctPMI.add(p.getRightElement());
      }
    }

    System.out.println("Number of distict PMI's " + distinctPMI.size());

    // sorting both the lists
    cloudList = AnalyzePMI.bubblesort(cloudList);
    loveList = AnalyzePMI.bubblesort(loveList);

    System.out.println("\n TOP THREE WORDS WITH CLOUD\n");

    for (int i = 0; i <= 2; i++) {
      System.out.println(cloudList.get(i).getLeftElement() + " "
          + cloudList.get(i).getRightElement().get());
    }

    System.out.println("\n TOP THREE WORDS WITH LOVE\n");

    for (int i = 0; i <= 2; i++) {
      System.out.println(loveList.get(i).getLeftElement() + " "
          + loveList.get(i).getRightElement().get());
    }
  }
}
