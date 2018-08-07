
package computation.seed_expansion;

import java.util.*;

import utils.TripleArrayListWritable;
import utils.ArrayPrimitiveWritable;

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

public class PersonalizedPageRankResult extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
    //ArrayList<Integer> communities = new ArrayList<Integer>();
    //MapWritable randomWalkMap = new MapWritable();
    //MapWritable originalMap = new MapWritable();
    //originalMap.put(new IntWritable(vData.getBestCommunity()), new IntWritable(1));
    int[] overlap = (int[]) vData.getOverlapCommunity().get();
    
    /*for (Map.Entry<Writable, Writable> e : p.entrySet()) {
      IntWritable srcId = (IntWritable) e.getKey();
      DoubleWritable pValue = (DoubleWritable) e.getValue();
      if (pValue.get() > 0.0) {
        communities.add(srcId.get());
        //randomWalkMap.put(srcId, new IntWritable(1));
      }
      //if (srcId.get() == 308943) { System.out.println(vertex.getId().get() + ", " + pValue.get()); }
    }*/
    
    //int[] arr = toPrimitiveArray(communities);
    //Arrays.sort(arr);
    //vData.setOverlapCommunity(new ArrayPrimitiveWritable(arr));
    
    //Collections.sort(communities);
    MapWritable comVerNum = new MapWritable(); // Community Vertices NumberAggregator
    /*for (int i = 0; i < communities.size(); i++) {
      comVerNum.put(new IntWritable(communities.get(i)), new IntWritable(1));
    }*/
    /*for (int i : overlap) {
      comVerNum.put(new IntWritable(i), new IntWritable(1));
    }*/
    comVerNum.put(new IntWritable(vData.getBestCommunity()), new IntWritable(1));
    aggregate(WccMasterCompute.COMMUNITY_VERTICES_NUMBER, comVerNum);
    //aggregate(WccMasterCompute.RANDOM_WALK_COMMUNITY_MEMBER, randomWalkMap);
    //aggregate(WccMasterCompute.ORIGINAL_COMMUNITY_MEMBER, originalMap);
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
