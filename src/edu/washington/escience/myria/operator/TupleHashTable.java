package edu.washington.escience.myria.operator;

import java.io.Serializable;
import java.util.List;

import com.gs.collections.api.iterator.IntIterator;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * An abstraction of a hash table of tuples.
 */
public final class TupleHashTable implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Map from hash codes to indices. */
  private transient IntObjectHashMap<IntArrayList> keyToIndices;
  /** The table containing keys and values. */
  private transient MutableTupleBuffer data;
  /** Key column indices. */
  private final int[] keyColumns;
  public String name;
  public int[] triggers = new int[] {};
  public int intc = 0, longc = 0, strc = 0;
  public long strsum = 0;
  public List<Type> types;

  /**
   * @param schema schema
   * @param keyColumns key column indices
   */
  public TupleHashTable(final Schema schema, final int[] keyColumns) {
    this.keyColumns = keyColumns;
    data = new MutableTupleBuffer(schema);
    keyToIndices = new IntObjectHashMap<IntArrayList>();
    types = schema.getColumnTypes();
    for (Type t : types) {
      if (t == Type.INT_TYPE || t == Type.FLOAT_TYPE) {
        intc += 1;
      }
      if (t == Type.LONG_TYPE || t == Type.DOUBLE_TYPE) {
        longc += 1;
      }
      if (t == Type.STRING_TYPE) {
        strc += 1;
      }
    }
  }

  /**
   * @return the number of tuples this hash table has.
   */
  public int numTuples() {
    return data.numTuples();
  }

  /**
   * Get the data table indices given key columns from a tuple in a tuple batch.
   *
   * @param tb the input tuple batch
   * @param key the key columns
   * @param row the row index of the tuple
   * @return the indices
   */
  public IntArrayList getIndices(final TupleBatch tb, final int[] key, final int row) {
    IntArrayList ret = new IntArrayList();
    IntArrayList indices = keyToIndices.get(HashUtils.hashSubRow(tb, key, row));
    if (indices != null) {
      IntIterator iter = indices.intIterator();
      while (iter.hasNext()) {
        int i = iter.next();
        if (TupleUtils.tupleEquals(tb, key, row, data, keyColumns, i)) {
          ret.add(i);
        }
      }
    }
    return ret;
  }

  /**
   * Replace tuples in the hash table with the input tuple if they have the same key.
   *
   * @param tb the input tuple batch
   * @param keyColumns the key columns
   * @param row the row index of the input tuple
   * @return if at least one tuple is replaced
   */
  public boolean replace(final TupleBatch tb, final int[] keyColumns, final int row) {
    IntIterator iter = getIndices(tb, keyColumns, row).intIterator();
    if (!iter.hasNext()) {
      return false;
    }
    while (iter.hasNext()) {
      int i = iter.next();
      for (int j = 0; j < data.numColumns(); ++j) {
        if (types.get(j) == Type.STRING_TYPE) {
          String ot = tb.getString(j, i);
          String nt = tb.getString(j, row);
          strsum = strsum - ot.length() + nt.length();
        }
        data.replace(j, i, tb.getDataColumns().get(j), row);
      }
    }
    return true;
  }

  /**
   * @param tb tuple batch of the input tuple
   * @param keyColumns key column indices
   * @param row row index of the input tuple
   * @param keyOnly only add keyColumns
   */
  public void addTuple(
      final TupleBatch tb, final int[] keyColumns, final int row, final boolean keyOnly) {
    int hashcode = HashUtils.hashSubRow(tb, keyColumns, row);
    IntArrayList indices = keyToIndices.get(hashcode);
    if (indices == null) {
      indices = new IntArrayList();
      keyToIndices.put(hashcode, indices);
    }
    indices.add(numTuples());
    if (keyOnly) {
      for (int i = 0; i < keyColumns.length; ++i) {
        if (types.get(i) == Type.STRING_TYPE) {
          strsum += tb.getString(keyColumns[i], row).length();
        }
        data.put(i, tb.getDataColumns().get(keyColumns[i]), row);
      }
    } else {
      for (int i = 0; i < data.numColumns(); ++i) {
        if (types.get(i) == Type.STRING_TYPE) {
          strsum += tb.getString(i, row).length();
        }
        data.put(i, tb.getDataColumns().get(i), row);
      }
    }
    if (triggers != null) {
      for (int t : triggers) {
        if (t == numTuples()) {
          System.out.println("sysgcopstats\t" + dumpStats());
          System.gc();
        }
      }
    }
  }

  /**
   * @return the data
   */
  public MutableTupleBuffer getData() {
    return data;
  }

  /**
   * Clean up the hash table.
   */
  public void cleanup() {
    // System.out.println("cleanup " + dumpStats());
    // keyToIndices = new IntObjectHashMap<IntArrayList>();
    // data = new MutableTupleBuffer(data.getSchema());
  }

  public String dumpStats() {
    int n = numTuples();
    // name = "hash";
    return name
        + " "
        + numTuples()
        + " "
        + keyToIndices.size()
        + " "
        + (n == 0 ? 0 : longc)
        + " "
        + (n == 0 ? 0 : strc)
        + " "
        + (n == 0 ? 0 : strsum / n);
  }

  public Schema getSchema() {
    return data.getSchema();
  }
}
