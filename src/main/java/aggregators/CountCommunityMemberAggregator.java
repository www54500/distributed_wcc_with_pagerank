
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

public class CountCommunityMemberAggregator extends BasicAggregator<MapWritable> {

  @Override
  public void aggregate(MapWritable m) {
    if (!m.isEmpty()) {
      // Get current aggregated map
      MapWritable current = getAggregatedValue();
      for (Map.Entry<Writable, Writable> e : m.entrySet()) {
        IntIntPairWritable communityPair = (IntWritable) e.getKey();
        IntWritable cut = (IntWritable) e.getValue();
        //int commNodeNumber = ((IntWritable) e.getValue()).get();
        if (!current.containsKey(community)) {
          current.put(communityPair, cut);
        } else {
          IntWritable currentCut = (IntWritable) current.get(community);
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
