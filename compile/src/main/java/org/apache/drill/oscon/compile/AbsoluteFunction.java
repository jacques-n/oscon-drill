package org.apache.drill.oscon.compile;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.LongHolder;
import org.apache.drill.exec.record.RecordBatch;

public class AbsoluteFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbsoluteFunction.class);
  
  /**
   * Inform drill about the existence of the Function Definition via class path scanning.
   */
  public static class FuncProvider implements CallProvider{
    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[]{
          FunctionDefinition.operator("abs", new ArgumentValidators.NumericTypeAllowed(1, Integer.MAX_VALUE, true), new OutputTypeDeterminer.SameAsAnySoft(), "abs")
      };
    }
  }
  
  /** 
   * Define the actual absolute value implementation
   */
  @FunctionTemplate(name = "abs", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class AbsoluteImpl implements DrillFunc{
    
    @Param LongHolder input;
    @Output LongHolder out;

    public void setup(RecordBatch b){}
    
    public void eval(){
      out.value = Math.abs(input.value);
    }

  }

}
