package org.apache.drill.oscon.compile;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.expression.OutputTypeDeterminer.FixedType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.record.RecordBatch;

public class SinFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SinFunction.class);
  
  /**
   * Inform drill about the existence of the Function Definition via class path scanning.
   */
  public static class FuncProvider implements CallProvider{
    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[]{
          FunctionDefinition.operator("sin", new ArgumentValidators.NumericTypeAllowed(1, 2, true), // 
              new OutputTypeDeterminer.SameAsAnySoft(), "sin")
      };
    }
  }
  
  /** 
   * Define the actual absolute value implementation
   */
  @FunctionTemplate(name = "sin", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class AbsoluteImpl implements DrillFunc{
    
    @Param Float8Holder input;
    @Output Float8Holder out;

    public void setup(RecordBatch b){}
    
    public void eval(){
      out.value = Math.sin(input.value);
    }

  }

}
