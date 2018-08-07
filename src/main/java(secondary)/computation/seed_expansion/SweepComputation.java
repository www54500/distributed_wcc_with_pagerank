
package computation.seed_expansion;

import computation.WccMasterCompute;

import vertex.WccVertexData;

import utils.IntegerIntegerDoubleTripleWritable;
import utils.TripleArrayListWritable;

import messages.PagerankPushMessage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

public class SweepComputation extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
    int degree = vertex.getNumEdges();

    if (p.containsKey(sweepComm) && ((DoubleWritable) p.get(sweepComm)).get() > 0.0) { // contains sweep community and that p value not zero
      double pValue = ((DoubleWritable) p.get(sweepComm)).get();
      double newP = pValue / (double) degree;
      vData.getPagerank().put(sweepComm, new DoubleWritable(newP));
      IntegerIntegerDoubleTripleWritable triple = new IntegerIntegerDoubleTripleWritable(vertex.getId().get(), degree, newP);
      TripleArrayListWritable pair = new TripleArrayListWritable();
      pair.add(triple);
      //System.out.println(vertex.getId().get() + ": sweepcomputation, pair.size = " + pair.size());
      aggregate("sweep", pair);
    }
  }
}
