
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;

import utils.TripleArrayListWritable;
import utils.IntegerIntegerDoubleTripleWritable;

public class TripleArrayListAggregator extends BasicAggregator<TripleArrayListWritable> {

  @Override
  public void aggregate(TripleArrayListWritable arrList) {
    TripleArrayListWritable curArrlist = getAggregatedValue();
    for (IntegerIntegerDoubleTripleWritable iidt : arrList) {
      curArrlist.add(iidt);
    }
    /*if (arrList.size() == 1) {
      curArrlist.add(arrList.get(0));
    } else {
      System.out.println("PairArrayListAggregator: arrList.size() is " + arrList.size());
    }*/
    setAggregatedValue(curArrlist);    
  }

  @Override
  public TripleArrayListWritable createInitialValue() {
    return new TripleArrayListWritable();
  }
}
