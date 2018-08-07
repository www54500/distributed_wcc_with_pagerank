
package computation.seed_expansion;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.PagerankPushMessage;

import java.util.*;

import utils.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

public class computeConductance extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
  public void preSuperstep() {}

  //TODO: Put in superclass
  @Override
  public void postSuperstep() {
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
  }

  @Override
  public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex, Iterable<PagerankPushMessage> messages) {
    WccVertexData vData = vertex.getValue();
    MapWritable topComm = (MapWritable) getAggregatedValue(WccMasterCompute.TOP_COMMUNITIES);
    MapWritable ncm = vData.getNeighborCommunityMap();
    MapWritable cutMap = new MapWritable();
    MapWritable volumeMap = new MapWritable();
    int[] finalComm = (int[]) vData.getFinalCommunity().get();
    HashMap<Integer, Integer> ncc = new HashMap<Integer, Integer>(); // neighborCommunityCount
    
    // initial finalComm
    if (finalComm.length == 0) {
      int[] arr = new int[]{vData.getBestCommunity()};
      Arrays.sort(arr);
      finalComm = arr;
    }
    
    // count number of neighbor in other community to compute the cut
    for (Map.Entry<Writable,Writable> e : ncm.entrySet()) {
      int neighborCommunity = ((IntWritable) e.getValue()).get();
      if (ncc.containsKey(neighborCommunity)) {
        int i = ncc.get(neighborCommunity);
        ncc.put(neighborCommunity, i + 1);
      } else {
        ncc.put(neighborCommunity, 1);
      }
    }
    
    // compute cut and volume
    for (Map.Entry<Writable,Writable> e : topComm.entrySet()) {
      int primary = ((IntWritable) e.getKey()).get();
      int[] secondarys = (int[]) ((ArrayPrimitiveWritable) e.getValue()).get();
      if (Arrays.binarySearch(finalComm, primary) >= 0) { // i am primary
        int myCut = computeCut(primary, primary, ncc);
        cutMap.put(new IntIntPairWritable(primary, primary), new IntWritable(myCut));
      }
      for (int secondary : secondarys) {
        if (Arrays.binarySearch(finalComm, secondary) >= 0) { // i am secondarys
          int myCut = computeCut(secondary, secondary, ncc);
          cutMap.put(new IntIntPairWritable(secondary, secondary), new IntWritable(myCut));
        }
        
        if (Arrays.binarySearch(finalComm, primary) >= 0 || Arrays.binarySearch(finalComm, secondary) >= 0) { // cut of community after merge
          int CutWithSecond = computeCut(primary, secondary, ncc);
          cutMap.put(new IntIntPairWritable(primary, secondary), new IntWritable(CutWithSecond));
          
          // volume
          if (Arrays.binarySearch(finalComm, primary) >= 0) {
            volumeMap.put(new IntWritable(primary), new IntWritable(vertex.getNumEdges()));
          } 
          if (Arrays.binarySearch(finalComm, secondary) >= 0) { 
            volumeMap.put(new IntWritable(secondary), new IntWritable(vertex.getNumEdges()));
          }
        }
      }
    }
    
    // update cut and volume
    if (volumeMap.size() != 0) { aggregate(WccMasterCompute.COMMUNITY_VOLUME, volumeMap); }
    if (cutMap.size() != 0) { aggregate(WccMasterCompute.CUT, cutMap); }
  }
  
  private int computeCut(int com1, int com2, HashMap<Integer, Integer> ncc) {
    int myCut = 0;
    for (Map.Entry<Integer, Integer> el : ncc.entrySet()) {
      int community = el.getKey();
      int number = el.getValue(); // edges are connected other community
      if (community != com1 && community != com2) {
        myCut+=number;
      }
    }
    return myCut;
  }
}
