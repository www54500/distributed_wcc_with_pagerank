
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Writable;

import utils.TripleArrayListWritable;
import utils.IntegerIntegerDoubleTripleWritable;

public class MergeIIDAggregator extends BasicAggregator<TripleArrayListWritable> {

  @Override
  public void aggregate(TripleArrayListWritable tal) {
    if (!tal.isEmpty()) {
      //System.out.println("message size: " + tal.size());
      // Get current arraylist
      TripleArrayListWritable current = getAggregatedValue();
      for (IntegerIntegerDoubleTripleWritable iidt : tal) {
        int index = current.indexOf(iidt);
        if (index != -1) {
          IntegerIntegerDoubleTripleWritable iidtTemp = current.get(index);
          //int first = iidt.getFirst(); // first community
          //int second = iidt.getSecond(); // second community
          double third = iidt.getThird() + iidtTemp.getThird(); // number of vertices in both communities
          //current.remove(index);
          //System.out.printf("(%d, %d) = %d\n", first, second, (int) third);
          //current.add(new IntegerIntegerDoubleTripleWritable(first, second, third));
          current.get(index).setThird(third);
        } else {
          //System.out.printf("(%d, %d) = %d\n", iidt.getFirst(), iidt.getSecond(), (int) iidt.getThird());
          current.add(iidt);
        }
      }
    }
  }

  @Override
  public TripleArrayListWritable createInitialValue() {
    return new TripleArrayListWritable();
  }
}
