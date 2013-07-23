package org.apache.drill.oscon.compile;

public abstract class OutsideTemplate implements OutsideInterface, InsideInterface{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutsideTemplate.class);

  @Override
  public long getSum() {
    long sum = 0;
    for(int i = 0; i < 5; i++){
      sum+= eval();
    }
    return sum;
  }

  @Override
  public abstract long eval();
  
}
