package com.datatorrent.lib.streamquery.function;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;



/**
 * <p> An implementation of sql sum function. </p>
 * <p>
 * @displayName Sum Function
 * @category Streamquery/Functions
 * @tags sql sum, aggregate
 * @since 0.3.4
 */
public class SumFunction extends FunctionIndex
{
  public SumFunction(String column, String alias) throws Exception
  {
    super(column, alias);
  }

  @Override
  public Object compute(@NotNull ArrayList<Map<String, Object>> rows) throws Exception
  {
    Double result = 0.0;
    for (Map<String, Object> row : rows) {
        if (!row.containsKey(column)) continue;
        result += ((Number)row.get(column)).doubleValue();
    }
    return result;
  }

  @Override
  protected String aggregateName()
  {
   return "Sum(" + column;
  }

}
