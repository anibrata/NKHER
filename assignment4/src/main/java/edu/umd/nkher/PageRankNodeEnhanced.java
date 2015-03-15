package edu.umd.nkher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

/*
 * Source code from Cloud9: https://github.com/lintool/Cloud9
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */

public class PageRankNodeEnhanced implements Writable {

  public static enum Type {
    Complete((byte) 0), // PageRank mass and adjacency list
    Mass((byte) 1), // PageRank mass only
    Structure((byte) 2); // Adjacency list only

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  }
  
  // fields
  private static final Type[] mapping = new Type[] {Type.Complete, Type.Mass, Type.Structure};
  private Type type;
  private int nodeid;
  private ArrayListOfFloatsWritable pagerankList;
  private ArrayListOfIntsWritable adjacencyList;
  
  // empty constructor
  public PageRankNodeEnhanced() {
  }
  

  public ArrayListOfFloatsWritable getPageRank() {
    return pagerankList;
  }

  public void setPageRank(ArrayListOfFloatsWritable list) {
    this.pagerankList = list;
  }

  public int getNodeId() {
    return nodeid;
  }

  public void setNodeId(int n) {
    this.nodeid = n;
  }

  public ArrayListOfIntsWritable getAdjacencyList() {
    return adjacencyList;
  }

  public void setAdjacencyList(ArrayListOfIntsWritable list) {
    this.adjacencyList = list;
  }
  
  public Type getType() {
    return type;
  }

  public void setType(Type t) {
    this.type = t;
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int b = in.readByte();
    type = mapping[b]; // can be 0, 1 or 2
    nodeid = in.readInt();

    if (type.equals(Type.Mass)) {
      pagerankList = new ArrayListOfFloatsWritable();
      pagerankList.readFields(in);
      return;
    }

    if (type.equals(Type.Complete)) {
      pagerankList = new ArrayListOfFloatsWritable();
      pagerankList.readFields(in);
    }

    // Reading the adjacency list
    adjacencyList = new ArrayListOfIntsWritable();
    adjacencyList.readFields(in);
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.val);
    out.writeInt(nodeid);

    if (type.equals(Type.Mass)) {
      pagerankList.write(out);
      return;
    }

    if (type.equals(Type.Complete)) {
      pagerankList.write(out);
    }

    adjacencyList.write(out);
  }

  @Override
  public String toString() {
    return String.format("{%d, %s, %s}", nodeid, 
        (pagerankList == null ? "[]" : pagerankList.toString()),
        (adjacencyList == null ? "[]" : adjacencyList.toString(10)));
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this
   *         object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);
    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNodeEnhanced create(DataInput in) throws IOException {
    PageRankNodeEnhanced node = new PageRankNodeEnhanced();
    node.readFields(in);
    return node;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNodeEnhanced create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}



