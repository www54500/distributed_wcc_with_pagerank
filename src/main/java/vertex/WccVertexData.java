
package vertex;

import messages.CommunityInitMessageArrayWritable;
import messages.CommunityInitializationMessage;
import computation.WccMasterCompute;
import utils.ArrayPrimitiveWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.lang.Math;
import java.util.*;
import java.text.DecimalFormat;

public class WccVertexData implements Writable {
  /**
   * The number of triangles x closes in the graph, i.e. t(x, V)
   */
  private int t;

  /**
   * The number of vertices that form at least one triangle with x, 
   * i.e. vt(x, V)
   */
  private int vt;

  private int communityT;

  private int communityVt;
  
  private int sweepPlace;
  
  private int mergeComm;
   
  /**
   * The local clustering coefficient of x
   */
  private double clusteringCoefficient;
  
  private double localWcc;
  
  //private double pagerank;
  
  private MapWritable pagerank;
  
  //private double residual;
  
  private MapWritable residual;

  /**
   * The identifier of the community to which the vertex beints
   */
  private int community;
  
  private int previousCommunity;

  private int bestCommunity;

  //private int bestSeedId;
  private MapWritable bestSeedId;
  
  //private int finalCommunity;
  private ArrayPrimitiveWritable finalCommunity;
  
  /**
   *  Used for community initialization phase
   */
  private CommunityInitMessageArrayWritable commInitNeighbors;

  /**
   *   A map from neighborId (IntWritable) to its community (IntWritable)
   */
  private MapWritable neighborCommunityMap;
  
  //private MapWritable countCommunityMap;

  /**
   *   An array containing the vertex's neighbors that have a higher degree.
   *   Used for preprocessing
   */
  private ArrayPrimitiveWritable higherDegreeNeighbors;

  /**
   *   An array containing the vertex's neighbors
   *   Used for preprocessing
   */
  private ArrayPrimitiveWritable neighbors;
  
  private ArrayPrimitiveWritable overlapCommunity;
  
  private ArrayPrimitiveWritable pushSeeds;
  
  //private boolean updateSeedList;

  public WccVertexData () {
    t = vt = communityT = communityVt = 0;
    community = previousCommunity = bestCommunity = WccMasterCompute.ISOLATED_COMMUNITY;
    commInitNeighbors = new CommunityInitMessageArrayWritable(new CommunityInitializationMessage[0]);
    neighborCommunityMap = new MapWritable();
    //countCommunityMap = new MapWritable();
    higherDegreeNeighbors = new ArrayPrimitiveWritable(new int[0]); 
    neighbors = new ArrayPrimitiveWritable(new int[0]);
    overlapCommunity = new ArrayPrimitiveWritable(new int[0]);
    pushSeeds = new ArrayPrimitiveWritable(new int[0]);
    finalCommunity = new ArrayPrimitiveWritable(new int[0]);
    localWcc = -1.0;
    pagerank = new MapWritable();
    residual = new MapWritable();
    sweepPlace = Integer.MAX_VALUE;
    mergeComm = 0;
    bestSeedId = new MapWritable();
    //updateSeedList = true;
  }

  public int getT() { return t; }
  public int getVt() { return vt; }
  public int getCommunityT() { return communityT; }
  public int getCommunityVt() { return communityVt; }
  public int getSweepPlace() { return sweepPlace; }
  //public boolean getUpdateSeedList() { return updateSeedList; }
  public double getClusteringCoefficient() { return clusteringCoefficient; }
  public double getLocalWcc() { return localWcc; }
  //public double getPagerank() { return pagerank; }
  public MapWritable getPagerank() { return pagerank; }
  //public double getResidual() { return residual; }
  public MapWritable getResidual() { return residual; }
  public int getCommunity() { return community; }
  public int getBestCommunity() { return bestCommunity; }  
  //public int getFinalCommunity() { return finalCommunity; }  
  //public int getBestSeedId() { return bestSeedId; }
  public MapWritable getBestSeedId() { return bestSeedId; }
  public int getMergeComm() { return mergeComm; }
  
  public CommunityInitMessageArrayWritable getCommInitNeighbors() { 
    return commInitNeighbors;
  }

  public MapWritable getNeighborCommunityMap() { return neighborCommunityMap; }
  
  //public MapWritable getCountCommunityMap() { return countCommunityMap; }

  public ArrayPrimitiveWritable getHigherDegreeNeighbors() {
    return higherDegreeNeighbors;
  }

  public ArrayPrimitiveWritable getNeighbors() {
    return neighbors;
  }

  public ArrayPrimitiveWritable getOverlapCommunity() {
    return overlapCommunity;
  }
  
  public ArrayPrimitiveWritable getPushSeeds() {
    return pushSeeds;
  }
  
  public ArrayPrimitiveWritable getFinalCommunity() {
    return finalCommunity;
  }
  
  public void updateCommunity(int c) { 
    this.previousCommunity = this.community;
    this.community = c; 
  }

  public void saveCurrentCommunityAsBest() {
    bestCommunity = community;
  }

  public void savePreviousCommunityAsBest() {
    bestCommunity = previousCommunity;
  }

  public void setT(int t) { this.t = t; }

  public void setVt(int vt) { this.vt = vt; }

  public void setCommunityT(int communityT) { this.communityT = communityT; }

  public void setCommunityVt(int communityVt) { this.communityVt = communityVt; }
  
  public void setSweepPlace(int sweepPlace) { this.sweepPlace = sweepPlace; }
  
  //public void setUpdateSeedList(boolean updateSeedList) { this.updateSeedList = updateSeedList; }

  // TODO: SHOULD ONLY BE USED FOR TESTING
  public void setCommunity(int community) {
    this.community = community;
  }

  // TODO: SHOULD ONLY BE USED FOR TESTING
  public void setBestCommunity(int bestCommunity) {
    this.bestCommunity = bestCommunity;
  }
  
  //public void setFinalCommunity(int finalCommunity) {
  //  this.finalCommunity = finalCommunity;
  //}

  //public void setBestSeedId(int bestSeedId) {
  //  this.bestSeedId = bestSeedId;
  //}
  
  public void setBestSeedId(MapWritable bestSeedId) {
    this.bestSeedId = bestSeedId;
  }
  
  public void setMergeComm(int mergeComm) {
    this.mergeComm = mergeComm;
  }

  public void setClusteringCoefficient(double cc) { 
    this.clusteringCoefficient = cc; 
  }
  
  public void setLocalWcc(double lwcc) { 
    this.localWcc = lwcc; 
  }
  
  //public void setPagerank(double p) { this.pagerank = p; }
  public void setPagerank(MapWritable p) { this.pagerank = p; }

  //public void setResidual(double r) { this.residual = r; }
  public void setResidual(MapWritable r) { this.residual = r; }
  
  public void setCommInitNeighbors(CommunityInitMessageArrayWritable sns) { 
    this.commInitNeighbors = sns; 
  }

  public void setNeighborCommunityMap(MapWritable ncm) { 
    this.neighborCommunityMap = ncm;
  }

  //public void setCountCommunityMap(MapWritable ccm) { 
  //  this.countCommunityMap = ccm;
  //}
  
  public void setHigherDegreeNeighbors(ArrayPrimitiveWritable hdns) {
    this.higherDegreeNeighbors = hdns;
  }

  public void setNeighbors(ArrayPrimitiveWritable ns) {
    this.neighbors = ns;
  }

  public void setOverlapCommunity(ArrayPrimitiveWritable oc) {
    this.overlapCommunity = oc;
  }
  
  public void setPushSeeds(ArrayPrimitiveWritable ps) {
    this.pushSeeds = ps;
  }
  
  public void setFinalCommunity(ArrayPrimitiveWritable fc) {
    this.finalCommunity = fc;
  }
  @Override 
  public void readFields(DataInput input) throws IOException {
    t                       = input.readInt();
    vt                      = input.readInt();
    communityT              = input.readInt();
    communityVt             = input.readInt();
    clusteringCoefficient   = input.readDouble();
    localWcc                = input.readDouble();
    //pagerank        = input.readDouble();
    //residual        = input.readDouble();
    community               = input.readInt();
    previousCommunity       = input.readInt();
    bestCommunity           = input.readInt();
    //finalCommunity          = input.readInt();
    //bestSeedId              = input.readInt();
    sweepPlace              = input.readInt();
    mergeComm               = input.readInt();
    //updateSeedList          = input.readBoolean();
    pagerank.readFields(input);
    residual.readFields(input);
    commInitNeighbors.readFields(input);
    neighborCommunityMap.readFields(input);
    //countCommunityMap.readFields(input);
    higherDegreeNeighbors.readFields(input); 
    neighbors.readFields(input); 
    overlapCommunity.readFields(input);
    pushSeeds.readFields(input);
    finalCommunity.readFields(input);
    bestSeedId.readFields(input);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(t);
    output.writeInt(vt);
    output.writeInt(communityT);
    output.writeInt(communityVt);
    output.writeDouble(clusteringCoefficient);
    output.writeDouble(localWcc);
    //output.writeDouble(pagerank);
    //output.writeDouble(residual);
    output.writeInt(community);
    output.writeInt(previousCommunity);
    output.writeInt(bestCommunity);
    //output.writeInt(finalCommunity);
    //output.writeInt(bestSeedId);
    output.writeInt(sweepPlace);
    output.writeInt(mergeComm);
    //output.writeBoolean(updateSeedList);
    pagerank.write(output);
    residual.write(output);
    commInitNeighbors.write(output); 
    neighborCommunityMap.write(output); 
    //countCommunityMap.write(output); 
    higherDegreeNeighbors.write(output);
    neighbors.write(output);
    overlapCommunity.write(output);
    pushSeeds.write(output);
    finalCommunity.write(output);
    bestSeedId.write(output);
  }

  @Override
  public String toString() {
    //return "(t = " + t + ", vt = " + vt + ", clusteringCoefficient = " +
      //clusteringCoefficient + ", community = " + community + ", bestCommunity = " + bestCommunity + ")" ;
    //return String.valueOf(bestCommunity);
    /*---------------------------------------------------------
    String output = "";
    for (Writable w : bestSeedId.keySet()) {
      output += ((IntWritable) w).get() + "\t";
    }
    return output + bestCommunity;
    //---------------------------------------------------------*/
    /*random walk----------------------------------------------
    String output = "";
    DecimalFormat formatter = new DecimalFormat("#.###############");
    for (Map.Entry<Writable, Writable> e : pagerank.entrySet()) {
      IntWritable srcId = (IntWritable) e.getKey();
      DoubleWritable pValue = (DoubleWritable) e.getValue();
      output += String.valueOf(formatter.format(pValue.get())) + "\t";
    }
    if (pagerank.size() == 0) { output = "-1"; }
    //int[] overlap = (int[]) overlapCommunity.get();
    //for (int i : overlap) {
    //  output += String.valueOf(i) + "\t";
    //}
    return output;
    //---------------------------------------------------------*/
    /*---------------------------------------------------------
    int[] finalcom = (int[]) finalCommunity.get();
    boolean change = true;
    String output = "";
    if (bestCommunity != -1) {
      for (int i : finalcom) {
        output += i + "\t";
        if (i == bestCommunity) { change = false; }
      }
      //output += ";" + String.valueOf(bestCommunity) + "\t";
      output += "," + change;
    } else {
      output = "-1";
    }
    //output += ",";
    //int[] overlap = (int[]) overlapCommunity.get();
    //for (int i : overlap) {
    //  output += String.valueOf(i) + "\t";
    //}
    
    return output;
    //---------------------------------------------------------*/
    ///*---------------------------------------------------------
    int[] finalcom = (int[]) finalCommunity.get();
    String output = "";
    for (int i : finalcom) {
      output += i + "\t";
    }
    if (finalcom.length == 0) {
      return "-1";
    }
    return output;
    //---------------------------------------------------------*/
  }

//  @Override
//  public boolean equals(Object o) {
//    if (o instanceof WccVertexData) {
//      WccVertexData other = (WccVertexData) o;
//      double epsilon = 0.00000001;
//      return 
//        other.getT() == this.getT() &&
//        other.getVt() == this.getVt() &&
//        Math.abs(other.getClusteringCoefficient() -
//            this.getClusteringCoefficient()) < epsilon  &&
//        other.getCommunity() == this.getCommunity();
//    }
//    return false;
//  }
//
  @Override
  public int hashCode() {
    return 31*t + 31*vt + 31*community; 
  }
}
