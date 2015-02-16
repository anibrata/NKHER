package edu.umd.nkher;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

public class CountLines {

  File file;
  FileReader fileReader;
  LineNumberReader lnr;

  public int countlines(String pathToFile) {
    int rows = 0;
    file = new File(pathToFile);
    try {
      fileReader = new FileReader(file);
    } catch (FileNotFoundException e) {
      System.out.println("");
      e.printStackTrace();
    }
    try {
      lnr = new LineNumberReader(fileReader);
      while (lnr.readLine() != null) {
        rows++;
      }
    }
 catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      try {
        lnr.close();
        fileReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return rows;
  }

  public static void main(String[] args) {
    String path = args[0];
    CountLines countLines = new CountLines();
    System.out.println("Number of lines in " + path + " : " + countLines.countlines(path));
  }

}
