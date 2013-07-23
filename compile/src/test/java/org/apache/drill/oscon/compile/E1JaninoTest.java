package org.apache.drill.oscon.compile;

import static org.junit.Assert.assertTrue;

import org.codehaus.janino.ExpressionEvaluator;
import org.junit.Test;

/**
 * Do a simple Janino evaluation of a + b.
 */
public class E1JaninoTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(E1JaninoTest.class);
  
  // First we define a class to ease the movement back and forth from the typeless evaluator object.
  public static class Variables{
    public int a;
    public int b;
    public int out;
    
    public Variables(int a, int b) {
      super();
      this.a = a;
      this.b = b;
    }
  }
  
  @Test
  public void basicAddition() throws Exception{
    // set up an expression evaluator for setting the out field to the value of the sum of fields a and b.
    ExpressionEvaluator ee = new ExpressionEvaluator(
        "v.out = v.a + v.b",
        void.class,                     // expressionType
        new String[] { "v" },           // parameterNames
        new Class[] { Variables.class}  // parameterTypes
    );
    
    // define the input variable object.
    Variables v = new Variables(5, 10);
    
    // evaluate the expression.  void is response since we're putting the value back into the field.
    ee.evaluate(new Object[]{v});
    
    // check result.
    assertTrue(v.out == 15);
  }
}
