
package computation.seed_expansion;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.PagerankPushMessage;

import utils.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import java.util.*;

public class CollectingPrimaryCommunities extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {

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
    MapWritable topComm = (MapWritable) getAggregatedValue(WccMasterCompute.TOP_COMMUNITIES);
    MapWritable comVerNum = new MapWritable(); // Community Vertices Number Aggregator
    ArrayList<Integer> primary = new ArrayList<Integer>();
    TripleArrayListWritable primaryArr = new TripleArrayListWritable();
    
    // update Community Vertices Number Aggregator
    //for (int i : overlap) {
    //  comVerNum.put(new IntWritable(i), new IntWritable(1));
    //}
    
    // pick out primary community
    /*for (Map.Entry<Writable, Writable> e : topComm.entrySet()) {
      int i = ((IntWritable) e.getKey()).get();
      if (Arrays.binarySearch(overlap, i) > -1) {
        primary.add(i);
      }
    }*/
    for (int i : overlap) {
      if (topComm.containsKey(new IntWritable(i))) {
        primary.add(i);
      }
    }
    
    // update neighbor community map
    MapWritable ncm = vData.getNeighborCommunityMap();
    for (PagerankPushMessage m : messages) {
      int[] neighborId = m.getSeedId();
      double[] neighborBestComm = m.getNeighborUpdate();
      ncm.put(new IntWritable(neighborId[0]), new IntWritable((int) neighborBestComm[0]));
    }
    
    // overlap between primary and primary
    for (int i = 0; i < primary.size(); i++) {
      for (int j = i + 1; j < primary.size(); j++) {
        int firstComm = primary.get(i);
        int secondComm = primary.get(j);
        if (firstComm - secondComm != 0) {
          IntegerIntegerDoubleTripleWritable iidt = new IntegerIntegerDoubleTripleWritable();
          iidt.set(firstComm, secondComm, 1);
          primaryArr.add(iidt);
        }
      }
    }
    
    if (primaryArr.size() != 0) { aggregate(WccMasterCompute.PRIMARY_COMMUNITY_MERGE, primaryArr); }
    //aggregate(WccMasterCompute.COMMUNITY_VERTICES_NUMBER, comVerNum);
  }
}