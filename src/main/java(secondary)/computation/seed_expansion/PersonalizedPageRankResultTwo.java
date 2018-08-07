
package computation.seed_expansion;

import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

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

public class PersonalizedPageRankResultTwo extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
    MapWritable p = vData.getPagerank();
    ArrayList<Integer> communities = new ArrayList<Integer>();
    MapWritable mergeCommMap = new MapWritable();
    
    for (Map.Entry<Writable, Writable> e : p.entrySet()) {
      IntWritable srcId = (IntWritable) e.getKey();
      DoubleWritable pValue = (DoubleWritable) e.getValue();
      if (pValue.get() != 0.0) { 
        communities.add(srcId.get());
      }
    }
    Collections.sort(communities);
 
    for (int i = 0; i < communities.size(); i++) {
      for (int j = i; j < communities.size(); j++) {
        int firstComm = communities.get(i);
        int secondComm = communities.get(j);
        IntIntPairWritable pair = new IntIntPairWritable(firstComm, secondComm);
        if (vertex.getId().get() % 1000 == 0) { System.out.println(pair); }
        mergeCommMap.put(pair, new IntWritable(1));
      }
    }
    
    if (mergeCommMap.size() != 0) { aggregate(WccMasterCompute.COMMUNITY_NEED_MERGE, mergeCommMap); }
  }
}
