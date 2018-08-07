
package computation.seed_expansion;

import java.util.*;

import utils.IntegerIntegerDoubleTripleWritable;
import utils.TripleArrayListWritable;
import utils.ArrayPrimitiveWritable;
import utils.IntIntPairWritable;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.PagerankPushMessage;
import aggregators.MergeCommunityAggregator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import utils.*;

public class PersonalizedPageRankResultThree extends BasicComputation<IntWritable, WccVertexData, NullWritable, ArrayPrimitiveWritable> {
  
  private boolean finished;
  private int stepsToDo;
  private int currentStep;
  
  public void preSuperstep() {
    stepsToDo = ((IntWritable) getAggregatedValue(WccMasterCompute.NUMBER_OF_COLLECTING_STEPS)).get();
    currentStep = ((IntWritable) getAggregatedValue(WccMasterCompute.INTERPHASE_STEP)).get();
    finished = (currentStep == stepsToDo);
    if (!finished) { aggregate(WccMasterCompute.REPEAT_PHASE, new BooleanWritable(true)); }
  }

  //TODO: Put in superclass
  @Override
  public void postSuperstep() {
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
  }

  @Override
  public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex, Iterable<ArrayPrimitiveWritable> messages) {
    WccVertexData vData = vertex.getValue();
    int[] overlapCommunity = (int[]) vData.getOverlapCommunity().get();
    int[] arr = new int[0];
    boolean firstRound = getBroadcast("FirstRound");
    //boolean updateSeedListAll = false;
    boolean updateSeedList = (firstRound) ? true : vData.getUpdateSeedList();
    if (vertex.getId().get() == 1630) {
      System.out.println("PersonalizedPageRankResultThree: ");
      System.out.println("currentStep: " + currentStep);
      System.out.print("before:\t");
      for (int i : overlapCommunity) {
        System.out.print(i + "\t");
      }
      System.out.print("\n");
    }
    
    if (currentStep > 0) {
      arr = updateSeedList(vData, messages);
      if (vertex.getId().get() == 1630) {
        System.out.print("after :\t");
        for (int i : arr) {
          System.out.print(i + "\t");
        }
        System.out.print("\n");
      }
      updateSeedList = (arr.length != overlapCommunity.length);
      if (vertex.getId().get() == 1630) { System.out.print("two array are " + ((updateSeedList) ? "different" : "same") + "\n"); }
      vData.setUpdateSeedList(vData.getUpdateSeedList() || updateSeedList);
      //aggregate(WccMasterCompute.UPDATE_SEED_LIST_ALL, new BooleanWritable(updateSeedList));
    } else {
      vData.setUpdateSeedList(false);
    }
    
    if (!finished && updateSeedList) {
      sendSeedList(vertex, currentStep, stepsToDo);
    }
    //if (!keep) { aggregate(WccMasterCompute.COMMUNITY_NEED_MERGE, mergeCommMap); }   
    if (finished) {
      //boolean repeat = (updateSeedList || ((BooleanWritable) getAggregatedValue(WccMasterCompute.UPDATE_SEED_LIST)).get());
      boolean repeat = (updateSeedList || vData.getUpdateSeedList());
      if (vertex.getId().get() == 1630) { System.out.println("repeat: " + repeat); }
      if (repeat) { // keep going
        aggregate(WccMasterCompute.SEED_LIST_SENT, new LongWritable(arr.length));
        aggregate(WccMasterCompute.REPEAT_PHASE, new BooleanWritable(true));
        aggregate(WccMasterCompute.KEEP_COLLECTING, new BooleanWritable(true));
      } else { // next phase
        //aggregate(WccMasterCompute.PHASE_OVERRIDE, new BooleanWritable(true));
        //aggregate(WccMasterCompute.NEXT_PHASE, new IntWritable(WccMasterCompute.MERGE_COMMUNITIES));
      }
    }
  }
  
  private int[] updateSeedList(WccVertexData vData, Iterable<ArrayPrimitiveWritable> messages) {
    int[] overlapCommunity = (int[]) vData.getOverlapCommunity().get();
    // receive seed list of neighbor
    for (ArrayPrimitiveWritable arrWritable : messages) {
      int[] neighborOverlapCommunity = (int[]) arrWritable.get();
      if (!Arrays.equals(neighborOverlapCommunity, overlapCommunity)) { // not total same
        int[] mergeArray = compare(neighborOverlapCommunity, overlapCommunity);
        if (mergeArray.length < neighborOverlapCommunity.length + overlapCommunity.length) { // not total different
          overlapCommunity = mergeArray;
          Arrays.sort(overlapCommunity);
          vData.setOverlapCommunity(new ArrayPrimitiveWritable(overlapCommunity));
          // array updated, keep going
        }
      }
    }
    return overlapCommunity;
  }
  
  private void sendSeedList(Vertex<IntWritable, WccVertexData, NullWritable> vertex, int currentStep, int stepsToDo) {
    WccVertexData vData = vertex.getValue();
    int[] overlapCommunity = (int[]) vData.getOverlapCommunity().get();
    if (overlapCommunity.length != 0) {
      ArrayList<Integer> arrlist = new ArrayList<Integer>();
      for (int i = currentStep; i < overlapCommunity.length; i += stepsToDo) {
        arrlist.add(overlapCommunity[i]);
      }
      int[] arr = toPrimitiveArray(arrlist);
      sendMessageToAllEdges(vertex, new ArrayPrimitiveWritable(arr));
    }
  }
  
  private int[] compare(int[] arr1, int[] arr2) {
    ArrayList<Integer> arr = new ArrayList<Integer>();
    for (int i : arr1) {
      arr.add(i);
    }
    for (int i : arr2) {
      if (Arrays.binarySearch(arr1, i) < 0) { arr.add(i); }
    }
    int[] re = new int[arr.size()];
    for (int i = 0; i < arr.size(); i++) {
      re[i] = arr.get(i);
    }
    return re;
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
}
