
package messages;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommunityCenterMessage implements Writable {

  /**
   *  The id of the vertex sending the message
   */
  //private int sourceId;
  private int[] sourceId;

  /**
   *  The wccValue that the vertex transferred to
   */
  //private double wccValue;
  private double[] wccValue;

  public CommunityCenterMessage() {}

  //public CommunityCenterMessage(int sourceId, double wccValue) {
  //  this.sourceId = sourceId;
  //  this.wccValue = wccValue;
  //}
  public CommunityCenterMessage(int[] sourceId, double[] wccValue) {
    this.sourceId = sourceId;
    this.wccValue = wccValue;
  }

  public int[] getSourceId() { return sourceId; }

  public double[] getWccValue() { return wccValue; }

  @Override 
  public void readFields(DataInput input) throws IOException {
    int length = input.readInt();
    int[] arr = new int[length];
    double[] arr2 = new double[length];
    for (int i = 0; i < length; i++) {
      arr[i] = input.readInt();
      arr2[i] = input.readDouble();
    }
    sourceId = arr;
    wccValue = arr2;
    //sourceId = input.readInt();
    //wccValue = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(sourceId.length); // length
    for (int i = 0; i < sourceId.length; i++) {
      output.writeInt(sourceId[i]);
      output.writeDouble(wccValue[i]);
    }
    //output.writeInt(sourceId);
    //output.writeDouble(wccValue);
  }
  /*
  @Override
  public boolean equals(Object other) {
    if (other instanceof CommunityCenterMessage) {
      CommunityCenterMessage m = (CommunityCenterMessage) other;
      return m.getSourceId() == sourceId && m.getWccValue() == wccValue;
    } else return false;
  }

  @Override
  public int hashCode() {
    return 1 + sourceId*31 + (int) wccValue*31;
  }
  
  @Override
  public String toString() {
    return "CommunityCenterMessage(sourceId = " + sourceId + ", wccValue = " + wccValue +  ")" ; }
  */
}
