
package computation.seed_expansion;

import computation.WccMasterCompute;

import vertex.WccVertexData;

import utils.IntegerIntegerDoubleTripleWritable;
import utils.TripleArrayListWritable;

import messages.PagerankPushMessage;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

public class ConductanceComputation extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
      TripleArrayListWritable pair = getAggregatedValue("sweep");
      MapWritable ncm = vData.getNeighborCommunityMap();
      int degree = vertex.getNumEdges();
      int change = degree;

      int neighborPlace = -1;
      int myPlace = -1;
      for (Writable w : ncm.keySet()) {
        neighborPlace = -1;
        int neighborId = ((IntWritable) w).get();

        for (int i = 0; i < pair.size(); i++) {
          IntegerIntegerDoubleTripleWritable iidt = pair.get(i);
          if (neighborId == iidt.getFirst()) {
            neighborPlace = i;
          } else if (vertex.getId().get() == iidt.getFirst()) {
            myPlace = i;
            vData.setSweepPlace(myPlace);
          }
        }
        if (neighborPlace != -1 && neighborPlace < myPlace) {
          change -= 2;
        }
        //if (sweepComm.get() == 17 && vertex.getId().get() == 1) { System.out.println("my neighbor: " + neighborId + ", current change: " + change); }
        //if (sweepComm.get() == 17 && vertex.getId().get() == 1) { System.out.println("my neighbor place: " + neighborPlace + ", my place: " + myPlace); }
      }

      IntegerIntegerDoubleTripleWritable triple = new IntegerIntegerDoubleTripleWritable(myPlace, degree, (double) change);
      TripleArrayListWritable sweepPair = new TripleArrayListWritable();
      sweepPair.add(triple);
      //System.out.println(vertex.getId().get() + ": sweepcomputation, pair.size = " + sweepPair.size());
      aggregate("sweep", sweepPair);
    }
  }
  
  /*
  private int indexOfFirst(TripleArrayListWritable pair, int first) {
    for (int i = 0; i < pair.size; i++) {
      IntegerIntegerDoubleTripleWritable iidt = pair.get(i);
      if (first == iidt.getFirst()) {
        return i;
      }
    }
    
    return -1;
  }
  */
}
