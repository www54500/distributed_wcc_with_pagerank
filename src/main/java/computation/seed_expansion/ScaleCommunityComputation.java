
package computation.seed_expansion;

import computation.WccMasterCompute;

import vertex.WccVertexData;

import messages.PagerankPushMessage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

public class ScaleCommunityComputation extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
    IntWritable sweepComm = (IntWritable) getAggregatedValue("sweepComm");
    MapWritable p = vData.getPagerank();
    
    if (p.containsKey(sweepComm) && ((DoubleWritable) p.get(sweepComm)).get() > 0.0) {
      int myPlace = vData.getSweepPlace();
      int finalPlace = ((IntWritable) getBroadcast("finalPlace")).get();
      if (myPlace > finalPlace) {
        p.remove(sweepComm);
      }
    }
    
    MapWritable CommunityAggregator = getAggregatedValue(WccMasterCompute.COMMUNITY_AGGREGATES);
    if (!CommunityAggregator.isEmpty()) {
      // select any phase we want
      aggregate(WccMasterCompute.PHASE_OVERRIDE, new BooleanWritable(true));
      aggregate(WccMasterCompute.NEXT_PHASE, new IntWritable(WccMasterCompute.SWEEP_COMMUNITY));
    }
    
  }
}
