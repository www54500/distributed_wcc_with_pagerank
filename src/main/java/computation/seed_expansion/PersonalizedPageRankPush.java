
package computation.seed_expansion;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import utils.ArrayPrimitiveWritable;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.PagerankPushMessage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

public class PersonalizedPageRankPush extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
    //MapWritable pMap = vData.getPagerank();
    //MapWritable rMap = vData.getResidual();
    double epsilon = ((DoubleWritable) getBroadcast("epsilon")).get();
    System.out.println("epsilon:" + epsilon);
    double alpha = 0.99;
    double ratio = ((DoubleWritable) getAggregatedValue(WccMasterCompute.RATIO_OF_MEMORY)).get();
    long degree = vertex.getNumEdges();
    //ArrayList<Double> pArray = new ArrayList<Double>();
    //ArrayList<Double> rArray = new ArrayList<Double>();
    ArrayList<Integer> srcIdMsg = new ArrayList<Integer>();
    ArrayList<Double> neighborMsg = new ArrayList<Double>();
    //MapWritable msgMap = new MapWritable();
    
    // only access partial array to avoid out of memory
    ArrayPrimitiveWritable psWritable = vData.getPushSeeds();
    int[] psArr = (int[]) psWritable.get();
    int partial = (ratio < 1.0) ? (int) Math.floor((double) psArr.length * ratio) : psArr.length;
    //partial = (psArr.length > 5) ? 5 : psArr.length;
    int[] psArrLeft = Arrays.copyOfRange(psArr, partial, psArr.length);
    vData.setPushSeeds(new ArrayPrimitiveWritable(psArrLeft));
    for (int i = 0; i < partial; i++) {
      srcIdMsg.add(psArr[i]);
    }
    
    // push    
    if (srcIdMsg.size() != 0){
      int srcId = 0;
      double p = 0.0;
      double r = 0.0;
      double moving_probability = 0.0;
      double neighbor_update = 0.0;
      int[] overlap = (int[]) vData.getOverlapCommunity().get();
      ArrayList<Integer> updateP = new ArrayList<Integer>();
      for (int i = 0; i < srcIdMsg.size(); i++){
        srcId = srcIdMsg.get(i);
        p = (null == vData.getPagerank().get(new IntWritable(srcId))) ? 0.0 : ((DoubleWritable) vData.getPagerank().get(new IntWritable(srcId))).get();
        //System.out.printf("[%d][%d]%f", getSuperstep(), vertex.getId().get(), p);// test
        r = ((DoubleWritable) vData.getResidual().get(new IntWritable(srcId))).get();
        moving_probability = r - 0.5 * epsilon * (double) degree;
        r = 0.5 * epsilon * (double) degree;
        p += (1 - alpha) * moving_probability;
        //System.out.printf("->%f", p);// test
        neighbor_update = alpha * moving_probability / (double) degree;
        neighborMsg.add(neighbor_update);
        //pArray.add(p);
        //rArray.add(r);
        //double old_neighbor_update = (null == msgMap.get(new IntWritable(srcId))) ? 0.0 : ((DoubleWritable) msgMap.get(new IntWritable(srcId))).get();
        //msgMap.put(new IntWritable(srcId), new DoubleWritable(old_neighbor_update + neighbor_update));
        vData.getPagerank().put(new IntWritable(srcId), new DoubleWritable(p));
        vData.getResidual().put(new IntWritable(srcId), new DoubleWritable(r));
        if (p > 0 && Arrays.binarySearch(overlap, srcId) < 0) {
          if (!updateP.contains(new Integer(srcId))) {
            updateP.add(srcId);
          }
        }
      }
      // update overlap community array
      int[] newOverlap = new int[overlap.length + updateP.size()];
      int c = 0;
      for (int s : overlap) {
        newOverlap[c] = s;
        c++;
      }
      for (int s : updateP) {
        newOverlap[c] = s;
        c++;
      }
      Arrays.sort(newOverlap);
      vData.setOverlapCommunity(new ArrayPrimitiveWritable(newOverlap));
      //ArrayList<Integer> srcMsg = new ArrayList<Integer>();
      //for (Map.Entry<Writable, Writable> e : msgMap.entrySet()) {
      //  srcMsg.add(((IntWritable) e.getKey()).get());
      //  neighborMsg.add(((DoubleWritable) e.getValue()).get());
      //}
      
      //sendMessageToAllEdges(vertex, new PagerankPushMessage(srcMsg, neighborMsg));
      sendMessageToAllEdges(vertex, new PagerankPushMessage(srcIdMsg, neighborMsg));
      aggregate(WccMasterCompute.PHASE_OVERRIDE, new BooleanWritable(true));
      aggregate(WccMasterCompute.NEXT_PHASE, new IntWritable(WccMasterCompute.RANDOM_WALK));
    }

    //updatePagerank(vData, srcIdMsg, pArray);
    //updateResidual(vData, srcIdMsg, rArray);
    //vData.setPagerank(pMap);
    //vData.setResidual(rMap);
    aggregate("mapsize", new LongWritable(vData.getPagerank().size() + vData.getResidual().size()));
  }
  
  private void updatePagerank(WccVertexData vData, ArrayList<Integer> seedId, ArrayList<Double> pagerankValue) {
    for (int i = 0; i < seedId.size(); i++){
      vData.getPagerank().put(new IntWritable(seedId.get(i)), new DoubleWritable(pagerankValue.get(i)));
    }
  }
  
  private void updateResidual(WccVertexData vData, ArrayList<Integer> seedId, ArrayList<Double> residualValue) {
    for (int i = 0; i < seedId.size(); i++){
      vData.getResidual().put(new IntWritable(seedId.get(i)), new DoubleWritable(residualValue.get(i)));
    }
  }
}
