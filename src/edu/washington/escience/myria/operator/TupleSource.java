package edu.washington.escience.myria.operator;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleReader;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * This class creates a LeafOperator from a batch of tuples. Useful for testing.
 *
 *
 */
public final class TupleSource extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The class that will serialize the tuple batches. */
  private final TupleReader tupleReader;
  private final DataSource dataSource;

  /**
   * Instantiate a new DataInput operator, which will stream its tuples to the specified {@link TupleReader}.
   */
  public TupleSource(final TupleReader tupleReader, final DataSource dataSource) {
    this.tupleReader = tupleReader;
    this.dataSource = dataSource;
  }

  @Override
  protected void cleanup() throws DbException {
    try {
      tupleReader.close();
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    try {
      tupleReader.open(dataSource.getInputStream());
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    return tupleReader.readTuples();
  }

  @Override
  protected Schema generateSchema() {
    return tupleReader.getSchema();
  }
}
