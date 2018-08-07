
package computation.seed_expansion;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

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

public class PersonalizedPageRank extends BasicComputation<IntWritable, WccVertexData, NullWritable, PagerankPushMessage> {
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
    MapWritable rMap = vData.getResidual();
    double r_old = 0.0;
    double r_new = 0.0;
    double epsilon = ((DoubleWritable) getBroadcast("epsilon")).get();
    //double alpha = 0.99;
    long degree = vertex.getNumEdges();
    //ArrayList<Double> pArray = new ArrayList<Double>();
    ArrayList<Double> rArray = new ArrayList<Double>();
    ArrayList<Integer> srcIdMsg = new ArrayList<Integer>();
    //ArrayList<Double> neighborMsg = new ArrayList<Double>();
    //ArrayList<Double> tmpR = new ArrayList<Double>();
    
    for (PagerankPushMessage m : messages) {
      int[] seedId = m.getSeedId();
      double[] neighbor_update = m.getNeighborUpdate();
      ArrayList<Integer> tmpSeed = new ArrayList<Integer>();
      for (int i = 0; i < seedId.length; i++){
        r_old = (null == vData.getResidual().get(new IntWritable(seedId[i]))) ? 0.0 : ((DoubleWritable) rMap.get(new IntWritable(seedId[i]))).get();
        r_new = r_old + neighbor_update[i];
        
        //System.out.print("[" + vertex.getId().get() + "]:"); // for test
        //System.out.printf("%d: r_new = r_old + neighbor_update[i] = %.4f = %.4f + %.4f\n", seedId[i], r_new, r_old, neighbor_update[i]); 
        //System.out.printf("epsilon * degree = %.4f * %d = %.4f", epsilon, degree, epsilon * (double) degree);
                
        if (r_new > epsilon * (double) degree && r_old <= epsilon * (double) degree){ // push
          srcIdMsg.add(seedId[i]);
          //tmpR.add(r_new);// some r of seed s that need to push
        }
        //tmpSeed.add(seedId[i]);
        //rArray.add(r_new);// all r of seeds
        vData.getResidual().put(new IntWritable(seedId[i]), new DoubleWritable(r_new));
      }
      //vData.setResidual(rMap);
      //updateResidual(vData, tmpSeed, rArray);
    }
    
    int[] psArr = updatePushSeedsArray(vData.getPushSeeds(), srcIdMsg);    
    ArrayPrimitiveWritable psWritable = new ArrayPrimitiveWritable(psArr);
    vData.setPushSeeds(psWritable);
    //rArray.clear();
    aggregate(WccMasterCompute.PUSH_UPDATE_SENT, new LongWritable(psArr.length * degree));
  }
  
  private void updateResidual(WccVertexData vData, ArrayList<Integer> seedId, ArrayList<Double> residualValue) {
    for (int i = 0; i < seedId.size(); i++){
      vData.getResidual().put(new IntWritable(seedId.get(i)), new DoubleWritable(residualValue.get(i)));
    }
  }
  
  private int[] updatePushSeedsArray(ArrayPrimitiveWritable old_psWritable, ArrayList<Integer> srcIdMsg) {
    int[] psArr_old = (int[]) old_psWritable.get();
    int length = psArr_old.length;
    int[] psArr = new int[length + srcIdMsg.size()];
    for (int i = 0; i < length; i++) {
      psArr[i] = psArr_old[i];
    }
    for (int i = 0; i < srcIdMsg.size(); i++){
      psArr[i + length] = srcIdMsg.get(i).intValue();
    }
    
    return psArr;
  }
}
