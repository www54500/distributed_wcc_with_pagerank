
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import utils.IntIntPairWritable;

import java.util.Map;

public class MergeCommunityAggregatorTwo extends BasicAggregator<MapWritable> {

  @Override
  public void aggregate(MapWritable m) {
    if (!m.isEmpty()) {
      // Get current aggregated map
      MapWritable current = getAggregatedValue();
      for (Map.Entry<Writable, Writable> e : m.entrySet()) {
        IntIntPairWritable commPair = (IntIntPairWritable) e.getKey();
        IntWritable newVertices = (IntWritable) e.getValue();
        System.out.println(commPair);
        if (!current.containsKey(commPair)) {
          current.put(commPair, newVertices);
        } else {
          IntWritable currentVertices = (IntWritable) current.get(commPair);
          current.put(commPair, new IntWritable(currentVertices.get() + newVertices.get()));
        }
      }
    } 
  }

  @Override
  public MapWritable createInitialValue() {
    return new MapWritable();
  }
}
