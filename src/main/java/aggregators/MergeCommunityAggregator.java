
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

public class MergeCommunityAggregator extends BasicAggregator<MapWritable> {

  @Override
  public void aggregate(MapWritable m) {
    if (!m.isEmpty()) {
      // Get current aggregated map
      MapWritable current = getAggregatedValue();
      for (Map.Entry<Writable, Writable> e : m.entrySet()) {
        IntWritable firstComm = (IntWritable) e.getKey();
        MapWritable firstMap = (MapWritable) e.getValue();
        //int commNodeNumber = ((IntWritable) e.getValue()).get();
        if (!current.containsKey(firstComm)) {
          current.put(firstComm, firstMap);
        } else {
          MapWritable currentFirstMap = (MapWritable) current.get(firstComm);
          current.put(firstComm, merge(currentFirstMap, firstMap));
        }
      }
    } 
  }

  @Override
  public MapWritable createInitialValue() {
    return new MapWritable();
  }
  
  private MapWritable merge(MapWritable result, MapWritable part){
    for (Map.Entry<Writable, Writable> me : part.entrySet()){
      IntWritable commId = (IntWritable) me.getKey();
      if (result.containsKey(commId)){
        int currentNode = ((IntWritable) result.get(commId)).get();
        int newNode = ((IntWritable) me.getValue()).get();
        result.put(commId, new IntWritable(currentNode + newNode));
      } else {
        result.put(commId, (IntWritable) me.getValue());
      }              
    }
    return result;
  }
}
