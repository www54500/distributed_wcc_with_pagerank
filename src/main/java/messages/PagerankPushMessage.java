
package messages;

import org.apache.hadoop.io.Writable;

import java.util.ArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PagerankPushMessage implements Writable {

  /**
   *  The id of the vertex sending the message
   */
  //private int seedId;
  private int[] seedId = new int[0];

  /**
   *  The wccValue that the vertex transferred to
   */
  //private double wccValue;
  private double[] neighborUpdate = new double[0];

  public PagerankPushMessage() {}

  public PagerankPushMessage(int[] seedId, double[] neighborUpdate) {
    this.seedId = seedId;
    this.neighborUpdate = neighborUpdate;
  }
  
  public PagerankPushMessage(ArrayList<Integer> srcId, ArrayList<Double> neibor) {
    int[] seedId = new int[srcId.size()];
    for (int i = 0; i < srcId.size(); i++){
      seedId[i] = srcId.get(i);
    }
    double[] neighborUpdate = new double[neibor.size()];
    for (int i = 0; i < neibor.size(); i++){
      neighborUpdate[i] = neibor.get(i);
    }
    this.seedId = seedId;
    this.neighborUpdate = neighborUpdate;
  }

  public int[] getSeedId() { return seedId; }

  public double[] getNeighborUpdate() { return neighborUpdate; }

  @Override 
  public void readFields(DataInput input) throws IOException {
    int length = input.readInt();
    seedId = new int[length];
    for (int i = 0; i < seedId.length; i++){
      seedId[i] = input.readInt();
    }
    
    length = input.readInt();
    neighborUpdate = new double[length];
    for (int i = 0; i < neighborUpdate.length; i++){
      neighborUpdate[i] = input.readDouble();
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(seedId.length);
    for (int i = 0; i < seedId.length; i++){
      output.writeInt(seedId[i]);
    }
    
    output.writeInt(neighborUpdate.length);
    for (int i = 0; i < neighborUpdate.length; i++){
      output.writeDouble(neighborUpdate[i]);
    }
  }

  /*@Override
  public boolean equals(Object other) {
    if (other instanceof PagerankPushMessage) {
      PagerankPushMessage m = (PagerankPushMessage) other;
      return m.getseedId() == seedId && m.getWccValue() == wccValue;
    } else return false;
  }

  @Override
  public int hashCode() {
    return 1 + seedId*31 + (int) wccValue*31;
  }

  @Override
  public String toString() {
    return "PagerankPushMessage(seedId = " + seedId + ", wccValue = " + wccValue +  ")" ; }
  */
}
