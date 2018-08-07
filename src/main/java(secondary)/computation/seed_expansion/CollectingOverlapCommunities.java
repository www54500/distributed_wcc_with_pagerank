
package computation.seed_expansion;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.PagerankPushMessage;
import utils.ArrayPrimitiveWritable;
import utils.TripleArrayListWritable;
import utils.IntegerIntegerDoubleTripleWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import java.util.*;

public class CollectingOverlapCommunities extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {

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
    ArrayList<Integer> communities = new ArrayList<Integer>();
    ArrayList<Integer> primary = new ArrayList<Integer>();
    MapWritable topComm = (MapWritable) getAggregatedValue(WccMasterCompute.TOP_COMMUNITIES);
    TripleArrayListWritable commArr = new TripleArrayListWritable();
    
    // pick out primary and minor
    /*for (Map.Entry<Writable, Writable> e : topComm.entrySet()) {
      int i = ((IntWritable) e.getKey()).get();
      if (Arrays.binarySearch(overlap, i) > -1) {
        primary.add(i);
      } else {
        communities.add(i);
      }
    }*/
    // update neighbor community map
    MapWritable ncm = vData.getNeighborCommunityMap();
    for (PagerankPushMessage m : messages) {
      int[] neighborId = m.getSeedId();
      double[] neighborBestComm = m.getNeighborUpdate();
      ncm.put(new IntWritable(neighborId[0]), new IntWritable((int) neighborBestComm[0]));
    }    
    /*test 1 hop node------------
    if (topComm.containsKey(new IntWritable(vData.getBestCommunity()))) {// i am primary
      for (Map.Entry<Writable,Writable> e : ncm.entrySet()) {
        IntWritable neighborCommunity = (IntWritable) e.getValue();
        if (vData.getBestCommunity() != neighborCommunity.get()) {
          IntegerIntegerDoubleTripleWritable iidt = new IntegerIntegerDoubleTripleWritable();
          iidt.set(vData.getBestCommunity(), neighborCommunity.get(), 1);
          commArr.add(iidt);
        }
      }      
    }
    //---------------------------*/
    
    for (int i : overlap) {
      if (topComm.containsKey(new IntWritable(i))) {
        primary.add(i);
      } else {
        communities.add(i);
      }
    }
    // overlap between primary and minor
    for (int firstComm : primary) {
      for (int secondComm : communities) {
        if (firstComm - secondComm != 0) {
          IntegerIntegerDoubleTripleWritable iidt = new IntegerIntegerDoubleTripleWritable();
          iidt.set(firstComm, secondComm, 1);
          commArr.add(iidt);
        }
      }
    }
    
    // collect Community Vertices Number Aggregator
    MapWritable comVerNum = new MapWritable();
    for (int i : overlap) {
      comVerNum.put(new IntWritable(i), new IntWritable(1));
    }
    
    if (commArr.size() != 0) { aggregate(WccMasterCompute.COMMUNITY_NEED_MERGE, commArr); }
    aggregate(WccMasterCompute.COMMUNITY_VERTICES_NUMBER, comVerNum);
  }
}

