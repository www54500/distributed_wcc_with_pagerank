
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

public class CommunityVerticesNumberAggregator extends BasicAggregator<MapWritable> {

  @Override
  public void aggregate(MapWritable m) {
    if (!m.isEmpty()) {
      // Get current aggregated map
      MapWritable current = getAggregatedValue();
      for (Map.Entry<Writable, Writable> e : m.entrySet()) {
        IntWritable community = (IntWritable) e.getKey();
        IntWritable verticesNumber = (IntWritable) e.getValue();
        if (current.containsKey(community)) {
          int currentNumber = ((IntWritable) current.get(community)).get();
          IntWritable newNumber = new IntWritable(currentNumber + verticesNumber.get());
          current.put(community, newNumber);
        } else {
          current.put(community, verticesNumber);
        }
      }
    } 
  }

  @Override
  public MapWritable createInitialValue() {
    return new MapWritable();
  }
}
