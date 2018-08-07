package utils;

import java.lang.Integer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class IntIntPairWritable implements Writable {
  private Integer first;
  private Integer second;
  
  public IntIntPairWritable() {}
  
  public IntIntPairWritable(Integer first, Integer second) {
    this.first = first;
    this.second = second;
  }
  
  public Integer getFirst() { return first; }
  public Integer getSecond() { return second; }
  public void setFirst() { this.first = first; }
  public void setSecond() { this.second = second; }
  
  @Override
  public int hashCode() { return first.hashCode() ^ second.hashCode(); }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IntIntPairWritable)) { return false; }
    IntIntPairWritable pairo = (IntIntPairWritable) o;
    return this.first.equals(pairo.getFirst()) && this.second.equals(pairo.getSecond());
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }
  
  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";
  }
}

