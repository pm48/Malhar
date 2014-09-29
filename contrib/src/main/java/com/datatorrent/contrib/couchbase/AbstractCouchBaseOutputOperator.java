package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import com.google.common.collect.Lists;
import java.util.List;

/**
 *
 * @author prerna
 * @param <T>
 */
public abstract class AbstractCouchBaseOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, CouchBaseWindowStore>
{

  private List<T> tuples;
  private transient Operator.ProcessingMode mode;
  protected static int num_tuples;

  public Operator.ProcessingMode getMode()
  {
    return mode;
  }

  public void setMode(Operator.ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractCouchBaseOutputOperator()
  {
    tuples = Lists.newArrayList();
    store = new CouchBaseWindowStore();
  }

  @Override
  public void setup(OperatorContext context)
  {
    mode = context.getValue(context.PROCESSING_MODE);
    if (mode == ProcessingMode.EXACTLY_ONCE) {
      throw new RuntimeException("This operator only supports atmost once and atleast once processing modes");
    }
    if (mode == ProcessingMode.AT_MOST_ONCE) {
      tuples.clear();
    }
    super.setup(context);
  }

  @Override
  public void processTuple(T tuple)
  {
    tuples.add(tuple);
  }

  public List<T> getTuples()
  {
    return tuples;
  }

  @Override
  public void storeAggregate()
  {
    num_tuples = tuples.size();
    for (T tuple: tuples) {
      insertOrUpdate(tuple);

    }
    tuples.clear();
  }

  public abstract String generatekey(T tuple);

  public abstract Object getObject(T tuple);

  protected abstract void insertOrUpdate(T tuple);

}
