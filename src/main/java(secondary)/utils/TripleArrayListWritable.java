package utils;

import utils.IntegerIntegerDoubleTripleWritable;

import org.apache.giraph.utils.ArrayListWritable;

public class TripleArrayListWritable extends ArrayListWritable<IntegerIntegerDoubleTripleWritable> {
  @Override
  @SuppressWarnings("unchecked")
  public void setClass() {
    setClass(IntegerIntegerDoubleTripleWritable.class);
  }
}