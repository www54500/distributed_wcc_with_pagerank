
package computation.seed_expansion;

import computation.WccMasterCompute;

import vertex.WccVertexData;

import messages.PagerankPushMessage;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;

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

public class MergeCommunitiesComputationTwo extends BasicComputation<IntWritable, WccVertexData, NullWritable, ArrayPrimitiveWritable> {
//public class MergeCommunitiesComputation extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
  public void preSuperstep() {}

  //TODO: Put in superclass
  @Override
  public void postSuperstep() {
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
  }

  @Override
  public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex, Iterable<ArrayPrimitiveWritable> messages) {
  //public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex, Iterable<PagerankPushMessage> messages) {
    WccVertexData vData = vertex.getValue();
    int[] overlapCommunity = (int[]) vData.getOverlapCommunity().get();
    int bestComm = vData.getBestCommunity();
    boolean found = false;
    if (overlapCommunity.length > 0) {
      for (int i : overlapCommunity) {
        if (bestComm == i) {
          vData.setFinalCommunity(overlapCommunity[0]);
          found = true;
        }
      }
      
      if (!found) {
        vData.setFinalCommunity(bestComm);
      }
    }
  }
}
