package org.apache.drill.oscon.compile;

import static org.junit.Assert.*;

import java.net.URL;
import java.util.Set;

import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.expr.SingleClassStringWriter;
import org.apache.drill.oscon.compile.OutsideInterface;
import org.apache.drill.oscon.compile.E2CodeModelTest.Adder;
import org.junit.Test;

import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;

public class E3TemplateMergeTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(E3TemplateMergeTest.class);

  @Test
  public void classTransformTest() throws Exception {
    ClassTransformer ct = new ClassTransformer();
    
    String className = "generated.template.Outside1";
    
    // get model and define class.
    JCodeModel model = new JCodeModel();
    JDefinedClass clazz = model._class(className);
    
    // create add method
    JMethod method = clazz.method(JMod.PUBLIC, long.class, "eval");
    method.body()._return(JExpr.lit(42));
        
    // output method as string.
    SingleClassStringWriter w = new SingleClassStringWriter();
    model.build(w);
    String classBody = w.getCode().toString();
    System.out.println(classBody);
    
    QueryClassLoader qcl = new QueryClassLoader(true);
    OutsideInterface i = ct.getImplementationClass(qcl, OutsideInterface.TEMPLATE_DEFINITION, classBody, className);
    assertEquals(i.getSum(), 210);
  }

}
