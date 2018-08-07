
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

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import utils.*;

public class InitializeMergeCommunities extends AbstractComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage, ArrayPrimitiveWritable> {
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
    ArrayList<Integer> communities = new ArrayList<Integer>();
    MapWritable p = vData.getPagerank();
    int[] overlapCommunity;

    for (Map.Entry<Writable, Writable> e : p.entrySet()) {
      IntWritable srcId = (IntWritable) e.getKey();
      DoubleWritable pValue = (DoubleWritable) e.getValue();
      if (pValue.get() != 0.0) { 
        communities.add(srcId.get());
      }
    }
    Collections.sort(communities);
    if (vertex.getId().get() == 1630) {
      System.out.println("degree:" + vertex.getNumEdges());
      System.out.println("InitializeMergeCommunities:");
      for (int i : communities) {
        System.out.print(i + "\t");
      }
      System.out.print("\n");
    }
    overlapCommunity = toPrimitiveArray(communities);
    vData.setOverlapCommunity(new ArrayPrimitiveWritable(overlapCommunity));    
    //sendMessageToAllEdges(vertex, new ArrayPrimitiveWritable(overlapCommunity));
    aggregate(WccMasterCompute.SEED_LIST_SENT, new LongWritable(overlapCommunity.length));
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
