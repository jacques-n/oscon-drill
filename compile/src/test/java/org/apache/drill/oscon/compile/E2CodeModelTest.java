package org.apache.drill.oscon.compile;

import static org.junit.Assert.assertTrue;

import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.expr.SingleClassStringWriter;
import org.junit.Test;

import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;


/**
 * Use CodeModel and Janino to build a new object that presents an expected interface.
 */
public class E2CodeModelTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(E2CodeModelTest.class);
  
  public interface Adder{
    public int add(int a, int b);
  }
  
  @Test
  public void basicAddition() throws Exception{
    
    String className = "generated.Example1";
    
    // get model and define class.
    JCodeModel model = new JCodeModel();
    JDefinedClass clazz = model._class(className);
    clazz._implements(Adder.class);
    
    // create add method
    JMethod method = clazz.method(JMod.PUBLIC, int.class, "add");
    JVar a = method.param(int.class, "a");
    JVar b = method.param(int.class, "b");
    method.body()._return(a.plus(b));
    
    // output method as string.
    SingleClassStringWriter w = new SingleClassStringWriter();
    model.build(w);
    String classBody = w.getCode().toString();
    System.out.println(classBody);
    
    // evaluate body as class, inject into a classloader for this purpose.
    QueryClassLoader qcl = new QueryClassLoader(true);
    byte[] bytecode = qcl.getClassByteCode(className, classBody);
    qcl.injectByteCode(className, bytecode);
    Class<?> classObject = qcl.loadClass(className);
    
    // check that the class carries the expected interface.
    assertTrue(Adder.class.isAssignableFrom(classObject));
    
    // create new instance and evaluate.
    Adder adder = (Adder) classObject.newInstance();
    assertTrue(adder.add(10, 5) == 15);
    
  }
}
