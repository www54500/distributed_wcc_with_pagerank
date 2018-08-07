
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;
import utils.*;

public class CommunityCutAggregator extends BasicAggregator<MapWritable> {

  @Override
  public void aggregate(MapWritable m) {
    if (!m.isEmpty()) {
      // Get current aggregated map
      MapWritable current = getAggregatedValue();
      for (Map.Entry<Writable, Writable> e : m.entrySet()) {
        IntIntPairWritable communityPair = (IntIntPairWritable) e.getKey();
        IntWritable cut = (IntWritable) e.getValue();
        if (!current.containsKey(communityPair)) {
          current.put(communityPair, cut);
        } else {
          IntWritable currentCut = (IntWritable) current.get(communityPair);
          current.put(communityPair, new IntWritable(currentCut.get() + cut.get()));
        }
      }
    } 
  }

  @Override
  public MapWritable createInitialValue() {
    return new MapWritable();
  }
}
