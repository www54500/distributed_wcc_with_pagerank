
package computation.seed_expansion;

import static computation.WccMasterCompute.ISOLATED_COMMUNITY;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.TransferMessage;
import messages.CommunityCenterMessage;
import aggregators.CommunityAggregatorData;
import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BooleanWritable;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import static java.lang.System.out;

import java.util.*;

public class GetSeedComputation extends AbstractComputation<IntWritable, WccVertexData, NullWritable, CommunityCenterMessage, CommunityCenterMessage> {

  private MapWritable highestVertex;

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
    int limitRound = ((IntWritable) getAggregatedValue(WccMasterCompute.LIMIT_ROUND)).get();
    WccVertexData vData = vertex.getValue();
    int community = vData.getBestCommunity();
    MapWritable ncm = vertex.getValue().getNeighborCommunityMap();
    this.highestVertex = vData.getBestSeedId();
    boolean update = (limitRound > 0) ? true : false;
    MapWritable comVerNum = (MapWritable) getAggregatedValue(WccMasterCompute.COMMUNITY_VERTICES_NUMBER);
    double verticeNumber = (double) ((IntWritable) comVerNum.get(new IntWritable(community))).get();
    MapWritable comVolume = (MapWritable) getAggregatedValue(WccMasterCompute.COMMUNITY_VOLUME);
    double volume = (double) ((IntWritable) comVolume.get(new IntWritable(community))).get();
    //int seedsNumber = (int) Math.floor((double) verticeNumber * 0.05);
    double averageDegree = volume / verticeNumber;
    double maxAverageDegree = ((DoubleWritable) getAggregatedValue(WccMasterCompute.MAX_AVERAGE_DEGREE)).get();
    aggregate(WccMasterCompute.MAX_AVERAGE_DEGREE, new DoubleWritable(averageDegree));
    int seedsNumber = (int) Math.round(Math.pow(averageDegree, 2) / Math.pow(maxAverageDegree, 1) * 2);// (average_degree^2 / max^1)
    if (seedsNumber > verticeNumber) { seedsNumber = (int) verticeNumber; }
    if (limitRound < 0) { seedsNumber += limitRound; }
    if (seedsNumber <= 0) { seedsNumber = 1; }
    
    MapWritable map = new MapWritable();
    map.put(new IntWritable(community), new IntWritable(1));
    aggregate(WccMasterCompute.COMMUNITY_VERTICES_NUMBER, map);
    
    for (CommunityCenterMessage m : messages) {
      int[] neighborIdArr = m.getSourceId();
      double[] neighborWccValueArr = m.getWccValue();
      for (int i = 0; i < neighborIdArr.length; i++) {
        if (highestVertex.size() < seedsNumber) {
          highestVertex.put(new IntWritable(neighborIdArr[i]), new DoubleWritable(neighborWccValueArr[i]));
          update = true;
        } else {
          boolean add = HigherWccValue(neighborIdArr[i], neighborWccValueArr[i]);
          update = (update || add);
        }
      }
    }
    if (this.highestVertex.size() > seedsNumber) {
      int del = this.highestVertex.size() - seedsNumber;
      reduceMap(del);
    }
    vData.setBestSeedId(this.highestVertex);
    
    if (update) {
      int[] neighborIdArr = new int[this.highestVertex.size()];
      double[] neighborWccValueArr = new double[this.highestVertex.size()];
      int i = 0;
      for (Map.Entry<Writable,Writable> e : this.highestVertex.entrySet()) {
        neighborIdArr[i] = ((IntWritable) e.getKey()).get();
        neighborWccValueArr[i] = ((DoubleWritable) e.getValue()).get();
        i++;
      }
      for (Map.Entry<Writable, Writable> e : ncm.entrySet()) {
        IntWritable neighbor = (IntWritable) e.getKey();
        int neighborComm = ((IntWritable) e.getValue()).get();
        out.print(neighborComm == -1 ? "have -1.\n" : "");
        if (neighborComm == community){
          sendMessage(neighbor, new CommunityCenterMessage(neighborIdArr, neighborWccValueArr));
        }
      }
      aggregate(WccMasterCompute.REPEAT_PHASE, new BooleanWritable(update));
    }

      /*
    int limitRound = ((IntWritable) getAggregatedValue(WccMasterCompute.LIMIT_ROUND)).get();
    WccVertexData vData = vertex.getValue();
    int community = vData.getBestCommunity();
    MapWritable ncm = vertex.getValue().getNeighborCommunityMap();
    
    // check the best (largest) wcc value from neighbor and update, then info to the other neighbor
    int highestVertex = (vData.getBestSeedId() == -1) ? vertex.getId().get() : vData.getBestSeedId();
    double highestWcc = vData.getLocalWcc();
    boolean findNewHighest = (limitRound > 0) ? true : false;
    //boolean findNewHighest = false;
    for (CommunityCenterMessage m : messages) {
      int neighborId = m.getSourceId();
      double neighborWccValue = m.getWccValue();
      if (neighborWccValue > highestWcc){
        highestVertex = neighborId;
        highestWcc = neighborWccValue;
        vData.setLocalWcc(neighborWccValue);
        vData.setBestSeedId(neighborId);
        //findNewHighest = true;
      } else if (neighborWccValue == highestWcc && neighborId > highestVertex){
        // if two node have same wcc value, both are seed.
        //highestVertex = neighborId;
        //findNewHighest = true;
        //vData.setBestSeedId(neighborId);
      } else {
        //System.out.println(vertex.getId().get() + "do nothing.");
      }
    }
    
    // if find the new best wcc then repeat the process
    if (findNewHighest == true && community != -1) {
      //aggregate(WccMasterCompute.REPEAT_PHASE, new BooleanWritable(true));
      ArrayList<IntWritable> neighborId = new ArrayList<IntWritable>();
      for (Map.Entry<Writable, Writable> e : ncm.entrySet()) {
        IntWritable neighbor = (IntWritable) e.getKey();
        int neighborComm = ((IntWritable) e.getValue()).get();
        if (neighborComm == community || neighborComm == -1){
          sendMessage(neighbor, new CommunityCenterMessage(highestVertex, highestWcc));
        }
      }
    } else {
      aggregate(WccMasterCompute.REPEAT_PHASE, new BooleanWritable(false));
      vData.setLocalWcc(highestWcc);
      vData.setBestSeedId(highestVertex);
    }
      */
  }

  private boolean HigherWccValue(int neighborId, double neighborWccValue) {
    boolean add = false;
    if (this.highestVertex.containsKey(new IntWritable(neighborId))) {
      return false;
    } else {
      for (Map.Entry<Writable,Writable> e : this.highestVertex.entrySet()) {
        int currentId = ((IntWritable) e.getKey()).get();
        double currentWcc = ((DoubleWritable) e.getValue()).get();
        if (neighborWccValue > currentWcc) {
          add = true;
          break;
        } else if (neighborWccValue == currentWcc && neighborId < currentId) {
          add = true;
          break;
        }
      }
    }
    if (add) {
      this.highestVertex.put(new IntWritable(neighborId), new DoubleWritable(neighborWccValue));
    }
    
    return add;
  }
  
  private void reduceMap(int del) {
    ArrayList<Map.Entry<Writable,Writable>> arr = new ArrayList<Map.Entry<Writable,Writable>>(this.highestVertex.entrySet());
    Collections.sort(arr, new Comparator<Map.Entry<Writable,Writable>>() {
      public int compare(Map.Entry<Writable,Writable> o1, Map.Entry<Writable,Writable> o2) {
        int o1key = ((IntWritable) o1.getKey()).get();
        int o2key = ((IntWritable) o2.getKey()).get();
        double o1value = ((DoubleWritable) o1.getValue()).get();
        double o2value = ((DoubleWritable) o2.getValue()).get();
        // we will get id smaller and wcc value biger, but reduce array take element from last, so sort array descending
        if (o1value > o2value) { return 1; }
        if (o1value < o2value) { return -1; }
        if (o1key > o2key) { return -1; }
        if (o1key < o2key) { return 1; }
        else { return 0; }
      }
    });
    
    ArrayList<Map.Entry<Writable,Writable>> reduceArr = new ArrayList<Map.Entry<Writable,Writable>>(arr.subList(del, arr.size()));//take from last
    MapWritable map = new MapWritable();
    for (Map.Entry<Writable,Writable> e : reduceArr) {
      map.put((IntWritable) e.getKey(), (DoubleWritable) e.getValue());
    }
    this.highestVertex = map;
  }
}

