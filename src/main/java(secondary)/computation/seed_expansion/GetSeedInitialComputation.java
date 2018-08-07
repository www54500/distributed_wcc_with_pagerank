
package computation.seed_expansion;

import static computation.WccMasterCompute.ISOLATED_COMMUNITY;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.TransferMessage;
import messages.CommunityCenterMessage;
import aggregators.CommunityAggregatorData;
import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BooleanWritable;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;

import static java.lang.System.out;

public class GetSeedInitialComputation extends AbstractComputation<IntWritable, WccVertexData, NullWritable, ArrayPrimitiveWritable, CommunityCenterMessage> {

  public void preSuperstep() {}

  //TODO: Put in superclass
  @Override
  public void postSuperstep() {
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
  }

  @Override
  public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex, Iterable<ArrayPrimitiveWritable> messages) {
    WccVertexData vData = vertex.getValue();
    double wccValue = vData.getLocalWcc();
    int community = vData.getBestCommunity();
    // sends wcc value to the same community neighbor
    MapWritable ncm = vData.getNeighborCommunityMap();
    LongWritable maxId = (LongWritable) getAggregatedValue(WccMasterCompute.MAX_VERTEX_ID);
    int degree = vertex.getNumEdges();
    
    if (community == -1) { // re-number the isolated community
      community = vertex.getId().get() + (int) maxId.get();
      vData.setBestCommunity(community);
    }
    
    int[] neighborIdArr = new int[]{vertex.getId().get()};
    double[] neighborWccValueArr = new double[]{wccValue};
    for (ArrayPrimitiveWritable m : messages) {
      int[] arr = (int[]) m.get();
      int neiborId = arr[0];
      int neiborBestComm = arr[1];
      if (neiborBestComm == -1) { neiborBestComm = neiborId + (int) maxId.get(); }
      ncm.put(new IntWritable(neiborId), new IntWritable(neiborBestComm));
      if (community == neiborBestComm) {
        sendMessage(new IntWritable(neiborId), new CommunityCenterMessage(neighborIdArr, neighborWccValueArr));
      }
    }
    vData.setNeighborCommunityMap(ncm);
    //----------------------------new----------------------------
    // every community vertex number
    MapWritable cvn = new MapWritable();
    cvn.put(new IntWritable(community), new IntWritable(1));
    aggregate(WccMasterCompute.COMMUNITY_VERTICES_NUMBER, cvn);
    MapWritable volume = new MapWritable();
    volume.put(new IntWritable(community), new IntWritable(degree));
    aggregate(WccMasterCompute.COMMUNITY_VOLUME, volume);
    
    // initial best vertex map
    vData.getBestSeedId().put(new IntWritable(vertex.getId().get()), new DoubleWritable(wccValue));
        
    aggregate(WccMasterCompute.PHASE_OVERRIDE, new BooleanWritable(true));
    aggregate(WccMasterCompute.NEXT_PHASE, new IntWritable(WccMasterCompute.FIND_COMMUNITY_SEED));
    /*
    WccVertexData vData = vertex.getValue();
    double wccValue = vData.getLocalWcc();
    int community = vData.getCommunity();
    int bestCommunity = vData.getBestCommunity();
    // sends wcc value to the same community neighbor
    MapWritable ncm = vertex.getValue().getNeighborCommunityMap();
    
    for (ArrayPrimitiveWritable m : messages) {
      int[] arr = (int[]) m.get();
      int neiborId = arr[0];
      int neiborBestComm = arr[1];
      ncm.put(new IntWritable(neiborId), new IntWritable(neiborBestComm));
      if (bestCommunity == neiborBestComm) {
      //System.out.println(neiborId + " and " + vertex.getId() + " are in the same community");
        sendMessage(new IntWritable(neiborId), new CommunityCenterMessage(vertex.getId().get(), wccValue));
      }
    }
    
    aggregate(WccMasterCompute.PHASE_OVERRIDE, new BooleanWritable(true));
    aggregate(WccMasterCompute.NEXT_PHASE, new IntWritable(WccMasterCompute.FIND_COMMUNITY_SEED));
    */
  }
}

