
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

public class MergeCommunitiesComputation extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
    int[] overlap = (int[]) vData.getOverlapCommunity().get();
    int bestComm = vData.getBestCommunity();
    int[] finalComm = (int[]) vData.getFinalCommunity().get();
    MapWritable topComm = (MapWritable) getAggregatedValue(WccMasterCompute.TOP_COMMUNITIES);
    IntWritable phaseAgg = getAggregatedValue(WccMasterCompute.PHASE);
    ArrayList<Integer> communities = new ArrayList<Integer>();
    ArrayList<Integer> finalList = new ArrayList<Integer>();
    boolean tellNeighbor = false; // temp
    
    if (finalComm.length == 0) {
      int[] arr = new int[]{bestComm};
      Arrays.sort(arr);
      finalComm = arr;
    }
    
    // update overlap community
    for (int i : overlap) {
      boolean find = false;
      for (Map.Entry<Writable,Writable> e : topComm.entrySet()) {
        int[] comms = (int[]) ((ArrayPrimitiveWritable) e.getValue()).get(); // be merged communities
        if (Arrays.binarySearch(comms, i) >= 0) {
          find = true;
          tellNeighbor = true;
          int key = ((IntWritable) e.getKey()).get();
          if (!communities.contains(new Integer(key))) {
            communities.add(key);
          }
        }
      }
      if (!find) {
        if (!communities.contains(new Integer(i))) {
            communities.add(i);
        }
      }
    }
    int[] arr = toPrimitiveArray(communities);
    Arrays.sort(arr);
    vData.setOverlapCommunity(new ArrayPrimitiveWritable(arr));
    
    // update final community
    for (int i : finalComm) {
      boolean find = false;
      for (Map.Entry<Writable,Writable> e : topComm.entrySet()) {
        int[] comms = (int[]) ((ArrayPrimitiveWritable) e.getValue()).get();
        if (Arrays.binarySearch(comms, i) >= 0) {
          find = true;
          int key = ((IntWritable) e.getKey()).get();
          if (!finalList.contains(new Integer(key))) {
            finalList.add(key);
          }
        }
      }
      if (!find) {
        if (!finalList.contains(new Integer(i))) {
          finalList.add(i);
        }
      }
    }
    int[] farr = toPrimitiveArray(finalList);    
    Arrays.sort(farr);
    vData.setFinalCommunity(new ArrayPrimitiveWritable(farr));
    
    // info neighbor my community change
    if (tellNeighbor) {
      double[] darr = new double[farr.length];
      for (int i = 0; i < farr.length; i++) {
        darr[i] = (double) farr[i];
      }
      sendMessageToAllEdges(vertex, new PagerankPushMessage(new int[]{vertex.getId().get()}, darr));
    }
    
    //if (phaseAgg.get() == WccMasterCompute.MERGE_PRIMARY_COMMUNITIES) {
    if (phaseAgg.get() == WccMasterCompute.MERGE_SECONDARY_COMMUNITIES) {
      BooleanWritable phaseOverride = (BooleanWritable) getAggregatedValue(WccMasterCompute.PHASE_OVERRIDE);
      IntWritable nextPhase = (IntWritable) getAggregatedValue(WccMasterCompute.NEXT_PHASE);
      aggregate(WccMasterCompute.PHASE_OVERRIDE, phaseOverride);
      aggregate(WccMasterCompute.NEXT_PHASE, nextPhase);
    }
  }
  
  private int[] toPrimitiveArray(ArrayList<Integer> arrList) {
    int[] arr = new int[arrList.size()];
    int i = 0;
    for (int x : arrList) {
      arr[i] = x;
      i++;
    }
    
    return arr;
  }
  
  private int[] updateFinalCommunity(int[] finalcomm, int merged, int key) {
    if (Arrays.binarySearch(finalcomm, merged) >= 0) {
      int[] arr = new int[0];
      if (Arrays.binarySearch(finalcomm, key) >= 0) { // already have key
        arr = new int[finalcomm.length - 1];
        int add = 0;
        for (int i : finalcomm) {
          if (i != merged) {
            arr[add] = i;
            add++;
          }          
        }
      } else {
        arr = new int[finalcomm.length];
        int add = 0;
        for (int i : finalcomm) {
          if (i == merged) { arr[add] = key; }
          else { arr[add] = i; }
        }
        add++;
      }
      return arr;
    }
    return finalcomm;
  }
}
