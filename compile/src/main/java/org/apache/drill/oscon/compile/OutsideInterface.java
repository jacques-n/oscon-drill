package org.apache.drill.oscon.compile;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.physical.impl.project.ProjectEvaluator;

public interface OutsideInterface {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutsideInterface.class);
  
  public long getSum();
  
  public static TemplateClassDefinition<OutsideInterface> TEMPLATE_DEFINITION = new TemplateClassDefinition<OutsideInterface>( //
      OutsideInterface.class, "org.apache.drill.oscon.compile.OutsideTemplate", ProjectEvaluator.class, long.class);

}
