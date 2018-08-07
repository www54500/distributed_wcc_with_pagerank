
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.util.*;

import utils.*;

public class MergeCommunityAggregatorThree extends BasicAggregator<MapWritable> {

  @Override
  public void aggregate(MapWritable m) {
    if (!m.isEmpty()) {
      // Get current aggregated map
      MapWritable current = getAggregatedValue();
      for (Map.Entry<Writable, Writable> e : m.entrySet()) {
        int[] arr = new int[0];
        boolean done = false;
        IntWritable key = (IntWritable) e.getKey();
        ArrayPrimitiveWritable commArrWritable = (ArrayPrimitiveWritable) e.getValue();
        int[] commArr = (int[]) commArrWritable.get();
        for (Map.Entry<Writable, Writable> ee : current.entrySet()) {
          ArrayPrimitiveWritable curCommArrWritable = (ArrayPrimitiveWritable) ee.getValue();
          int[] curCommArr = (int[]) curCommArrWritable.get();
          if (commArr.length == curCommArr.length) {
            if (Arrays.equals(commArr, curCommArr)) {
              done = true; // total same, dothing...
            }
          } 
        }
        if (!done) {
          for (Map.Entry<Writable, Writable> ee : current.entrySet()) {
            ArrayPrimitiveWritable curCommArrWritable = (ArrayPrimitiveWritable) ee.getValue();
            int[] curCommArr = (int[]) curCommArrWritable.get();
            int[] mergeArr = mergeArr(curCommArr, commArr);
            if (compare(mergeArr, curCommArr, commArr)) {
              key = (IntWritable) ee.getKey();
              arr = mergeArr;
              done = true; // partial same, update current map
              break;
            }
          }
          current.put(key, new ArrayPrimitiveWritable(arr));
        }
        if(!done) { // total different, add new array to map
          current.put(key, commArrWritable);
        }
      }
    } 
  }

  @Override
  public MapWritable createInitialValue() {
    return new MapWritable();
  }
  
  private int[] mergeArr(int[] arr1, int[] arr2) {
    Set<Integer> set = new TreeSet<Integer>();
    for (int i : arr1) {
      set.add(i);
    }
    for (int i : arr2) {
      set.add(i);
    }
    int[] arr = new int[set.size()];
    int i = 0;
    for (int e : set) {
      arr[i] = e;
      i++;
    }
    return arr;
  }
  
  private boolean compare(int[] arr, int[] arr1, int[] arr2) {
    if (arr.length < arr1.length + arr2.length) {
      return true;
    }
    if (arr.length == arr1.length + arr2.length) {
      return false;
    }
    return false;
  }
}
