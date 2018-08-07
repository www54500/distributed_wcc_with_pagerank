
package computation.seed_expansion;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.CommunityCenterMessage;
import messages.PagerankPushMessage;

import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

public class PersonalizedPageRankInitial extends AbstractComputation<IntWritable, WccVertexData, NullWritable, CommunityCenterMessage, PagerankPushMessage> {
  public void preSuperstep() {}

  //TODO: Put in superclass
  @Override
  public void postSuperstep() {
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
  }

  @Override
  public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex, Iterable<CommunityCenterMessage> messages) {
    WccVertexData vData = vertex.getValue();
    //int seedId = vData.getBestSeedId();
    MapWritable seedId = vData.getBestSeedId();
    int currentId = vertex.getId().get();
    double p = 0.0;
    double r = 0.0;
    int communityNumber = ((IntWritable) getAggregatedValue(WccMasterCompute.NUMBER_OF_COMMUNITIES)).get();
    long degree = vertex.getNumEdges();
    double epsilon = ((DoubleWritable) getBroadcast("epsilon")).get();
    double alpha = 0.99;
    int bestCommunity = vData.getBestCommunity();
    int[] overlap = new int[1];
    
    boolean isSeed = false;
    for (Writable w : seedId.keySet()) {
      if (((IntWritable) w).get() == currentId) {
        //r = (communityNumber == 0) ? 1.0 / 10.0 : 1.0/seedId.size();
        r = 1.0;
        isSeed = true;
      }
    }
    /* for test
    if (!isSeed) {
      MapWritable map = new MapWritable();
      map.put(new IntWritable(-1), new DoubleWritable(-1.0));
      vData.setBestSeedId(map);
    }
    */
    if (r > epsilon * (double) degree) { // push
      double moving_probability = r - 0.5 * epsilon * (double) degree;
      r = 0.5 * epsilon * (double) degree;
      p += (1 - alpha) * moving_probability;
      
      overlap[0] = bestCommunity;
      vData.setOverlapCommunity(new ArrayPrimitiveWritable(overlap));
      double neighbor_update = alpha * moving_probability / (double) degree;
      //int[] src = new int[]{vertex.getId().get()};
      int[] src = new int[]{bestCommunity};
      sendMessageToAllEdges(vertex, new PagerankPushMessage(src, new double[]{neighbor_update}));
    }
    //overlap[0] = bestCommunity;
    //vData.setOverlapCommunity(new ArrayPrimitiveWritable(overlap));
    if (p != 0.0 || r != 0.0) {
      updatePagerankMap(vertex, p);
      updateResidualMap(vertex, r);
      //System.out.printf("[%d][%d]%f\n", getSuperstep(), vertex.getId().get(), p); // test     
    }
    
  }
  
  private void updatePagerankMap(Vertex<IntWritable, WccVertexData, NullWritable> vertex, double pagerank) {
    WccVertexData vData = vertex.getValue();
    vData.getPagerank().put(new IntWritable(vData.getBestCommunity()), new DoubleWritable(pagerank));
    //vData.getPagerank().put(new IntWritable(vData.getCommunity()), new DoubleWritable(pagerank));
  }
  
  private void updateResidualMap(Vertex<IntWritable, WccVertexData, NullWritable> vertex, double residual) {
    WccVertexData vData = vertex.getValue();
    vData.getResidual().put(new IntWritable(vData.getBestCommunity()), new DoubleWritable(residual));
    //vData.getResidual().put(new IntWritable(vData.getCommunity()), new DoubleWritable(residual));
  }
}
