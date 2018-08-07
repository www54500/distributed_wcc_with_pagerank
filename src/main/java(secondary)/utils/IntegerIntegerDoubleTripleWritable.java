package utils;

import org.apache.hadoop.io.Writable;

import java.io.*;

//import utils.Pair;

@SuppressWarnings({"rawtypes"})
public class IntegerIntegerDoubleTripleWritable implements Writable {
  
  private int first;
  
  private int second;
  
  private double third;
  
  //private Pair<Integer, Double> value;

  public IntegerIntegerDoubleTripleWritable() {}
  
  public IntegerIntegerDoubleTripleWritable(int first, int second, double third) { set(first, second, third); }

  //public  IntegerIntegerDoubleTripleWritable(Pair<Integer, Double> value) { set(value); }

  public void set(int first, int second, double third) { 
    this.first = first; 
    this.second = second;
    this.third = third;
  }

  //public  Pair<Integer, Double> get() { return value; }
  
  public void setFirst(int first) { this.first = first; }
  
  public void setSecond(int second) { this.second = second; }

  public void setThird(double third) { this.third = third;; }

  public int getFirst() { return first; }
  
  public int getSecond() { return second; }

  public double getThird() { return third; }

  public void readFields(DataInput in) throws IOException {
    this.first = in.readInt();
    this.second = in.readInt();
    this.third = in.readDouble();
    //value = new Pair<Integer, Double>(first, second);
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
    out.writeDouble(third);
  }

  /** Returns true iff o is a IntegerIntegerDoubleTripleWritable with the same first value and second value. */
  public  boolean equals(Object o) {
    if (!(o instanceof IntegerIntegerDoubleTripleWritable)) {
      return false;
    }
    IntegerIntegerDoubleTripleWritable other = (IntegerIntegerDoubleTripleWritable) o;
    return this.first == other.first && this.second == other.second;
  }

  public  int hashCode() {
    return ((Integer) first).hashCode() ^ ((Integer) second).hashCode() ^ ((Double) third).hashCode();
  }

  public  String toString() {
    return Integer.toString(first) + "\t" + Integer.toString(second) + "\t" + Double.toString(third);
  }
}
