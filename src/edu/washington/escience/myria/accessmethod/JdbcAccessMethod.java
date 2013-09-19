package edu.washington.escience.myria.accessmethod;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ColumnBuilder;
import edu.washington.escience.myria.column.ColumnFactory;

/**
 * Access method for a JDBC database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcAccessMethod extends AccessMethod {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class);
  /** The database connection information. */
  private JdbcInfo jdbcInfo;
  /** The database connection. */
  private Connection jdbcConnection;

  /**
   * The constructor. Creates an object and connects with the database
   * 
   * @param jdbcInfo connection information
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  public JdbcAccessMethod(final JdbcInfo jdbcInfo, final Boolean readOnly) throws DbException {
    Objects.requireNonNull(jdbcInfo);
    this.jdbcInfo = jdbcInfo;
    connect(jdbcInfo, readOnly);
  }

  @Override
  public void connect(final ConnectionInfo connectionInfo, final Boolean readOnly) throws DbException {
    Objects.requireNonNull(connectionInfo);

    jdbcConnection = null;
    jdbcInfo = (JdbcInfo) connectionInfo;
    try {
      DriverManager.setLoginTimeout(5);
      /* Make sure JDBC driver is loaded */
      Class.forName(jdbcInfo.getDriverClass());
      jdbcConnection = DriverManager.getConnection(jdbcInfo.getConnectionString(), jdbcInfo.getProperties());
    } catch (ClassNotFoundException | SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void setReadOnly(final Boolean readOnly) throws DbException {
    Objects.requireNonNull(jdbcConnection);

    try {
      if (jdbcConnection.isReadOnly() != readOnly) {
        jdbcConnection.setReadOnly(readOnly);
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void tupleBatchInsert(final String insertString, final TupleBatch tupleBatch) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    try {
      /* Set up and execute the query */
      final PreparedStatement statement = jdbcConnection.prepareStatement(insertString);
      tupleBatch.getIntoJdbc(statement);
      // TODO make it also independent. should be getIntoJdbc(statement,
      // tupleBatch)
      statement.executeBatch();
      statement.close();
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString, final Schema schema)
      throws DbException {
    Objects.requireNonNull(jdbcConnection);
    try {
      /* Set up and execute the query */
      final Statement statement = jdbcConnection.createStatement();
      final ResultSet resultSet = statement.executeQuery(queryString);
      return new JdbcTupleBatchIterator(resultSet, schema);
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e);
    }
  }

  @Override
  public void close() throws DbException {
    /* Close the db connection. */
    try {
      jdbcConnection.close();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e);
    }
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void execute(final String ddlCommand) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    Statement statement;
    try {
      statement = jdbcConnection.createStatement();
      statement.execute(ddlCommand);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e);
    }
  }

  /**
   * Insert the Tuples in this TupleBatch into the database.
   * 
   * @param jdbcInfo information about the connection parameters.
   * @param insertString the insert statement. TODO No sanity checks at all right now.
   * @param tupleBatch the tupleBatch to be inserted.
   * @throws DbException if there is an error inserting these tuples.
   */
  public static void tupleBatchInsert(final JdbcInfo jdbcInfo, final String insertString, final TupleBatch tupleBatch)
      throws DbException {
    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, false);
    jdbcAccessMethod.tupleBatchInsert(insertString, tupleBatch);
    jdbcAccessMethod.close();
  }

  /**
   * Create a JDBC Connection and then expose the results as an Iterator<TupleBatch>.
   * 
   * @param jdbcInfo the JDBC connection information.
   * @param queryString the query.
   * @param schema the schema of the returned tuples.
   * @return an Iterator<TupleBatch> containing the results.
   * @throws DbException if there is an error getting tuples.
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final JdbcInfo jdbcInfo, final String queryString,
      final Schema schema) throws DbException {
    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, true);
    return jdbcAccessMethod.tupleBatchIteratorFromQuery(queryString, schema);
  }

  /**
   * Create a table with the given name and schema in the database. If dropExisting is true, drops an existing table if
   * it exists.
   * 
   * @param relationKey the name of the relation.
   * @param schema the schema of the relation.
   * @param dropExisting if true, an existing relation will be dropped.
   * @throws DbException if there is an error in the database.
   */
  @Override
  public void createTable(final RelationKey relationKey, final Schema schema, final boolean dropExisting)
      throws DbException {
    Objects.requireNonNull(jdbcConnection);
    Objects.requireNonNull(jdbcInfo);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);

    try {
      execute("DROP TABLE " + relationKey.toString(jdbcInfo.getDbms()) + ";");
    } catch (DbException e) {
      ; /* Skip. this is okay. */
    }
    execute(createStatementFromSchema(schema, relationKey));
  }

  /**
   * Create a table with the given name and schema in the database. If dropExisting is true, drops an existing table if
   * it exists.
   * 
   * @param jdbcInfo the JDBC connection information.
   * @param relationKey the name of the relation.
   * @param schema the schema of the relation.
   * @param dbms the DBMS, e.g., "mysql".
   * @param dropExisting if true, an existing relation will be dropped.
   * @throws DbException if there is an error in the database.
   */
  public static void createTable(final JdbcInfo jdbcInfo, final RelationKey relationKey, final Schema schema,
      final String dbms, final boolean dropExisting) throws DbException {
    Objects.requireNonNull(jdbcInfo);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);

    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, false);
    try {
      jdbcAccessMethod.execute("DROP TABLE " + relationKey.toString(dbms) + ";");
    } catch (DbException e) {
      ; /* Skip. this is okay. */
    }
    jdbcAccessMethod.execute(createStatementFromSchema(schema, relationKey, dbms));
    jdbcAccessMethod.close();
  }

  @Override
  public String insertStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(relationKey);
    final StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(relationKey.toString(jdbcInfo.getDbms())).append(" (");
    sb.append(StringUtils.join(schema.getColumnNames(), ','));
    sb.append(") VALUES (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('?');
    }
    sb.append(");");
    return sb.toString();
  }

  /**
   * Generates the create table statement string for a relation in the database.
   * 
   * @param schema the relation schema
   * @param relationKey the relation name
   * @param dbms the DBMS on which the table will be created
   * @return the create table statement string
   */
  public static String createStatementFromSchema(final Schema schema, final RelationKey relationKey, final String dbms) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(relationKey.toString(dbms)).append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(schema.getColumnName(i)).append(" ").append(typeToDbmsType(schema.getColumnType(i), dbms));
    }
    sb.append(");");
    return sb.toString();
  }

  @Override
  public String createStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(relationKey.toString(jdbcInfo.getDbms())).append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(schema.getColumnName(i)).append(" ")
          .append(typeToDbmsType(schema.getColumnType(i), jdbcInfo.getDbms()));
    }
    sb.append(");");
    return sb.toString();
  }

  /**
   * Generate a JDBC CREATE TABLE statement for the given table using the configured ConnectionInfo.
   * 
   * @param type a Myriad column type.
   * @return the name of the DBMS type that matches the given Myriad type.
   */
  public String typeToDbmsType(final Type type) {
    return typeToDbmsType(type, jdbcInfo.getDbms());
  }

  /**
   * Helper utility for creating JDBC CREATE TABLE statements.
   * 
   * @param type a Myriad column type.
   * @param dbms the description of the DBMS, e.g., "mysql".
   * @return the name of the DBMS type that matches the given Myriad type.
   */
  public static String typeToDbmsType(final Type type, final String dbms) {
    switch (type) {
      case BOOLEAN_TYPE:
        return "BOOLEAN";
      case DOUBLE_TYPE:
        return "DOUBLE";
      case FLOAT_TYPE:
        return "DOUBLE";
      case INT_TYPE:
        return "INTEGER";
      case LONG_TYPE:
        return "BIGINT";
      case STRING_TYPE:
        return "TEXT";
      case DATETIME_TYPE:
        return "TIMESTAMP";
      default:
        throw new UnsupportedOperationException("Type " + type + " is not supported");
    }
  }

}

/**
 * Wraps a JDBC ResultSet in a Iterator<TupleBatch>.
 * 
 * Implementation based on org.apache.commons.dbutils.ResultSetIterator. Requires ResultSet.isLast() to be implemented.
 * 
 * @author dhalperi
 * 
 */
class JdbcTupleBatchIterator implements Iterator<TupleBatch> {
  /** The results from a JDBC query that will be returned in TupleBatches by this Iterator. */
  private final ResultSet resultSet;
  /** The Schema of the TupleBatches returned by this Iterator. */
  private final Schema schema;
  /** Next TB. */
  private TupleBatch nextTB = null;
  /** statement is closed or not. */
  private boolean statementClosed = false;

  /**
   * Constructs a JdbcTupleBatchIterator from the given ResultSet and Schema objects.
   * 
   * @param resultSet the JDBC ResultSet containing the results.
   * @param schema the Schema of the generated TupleBatch objects.
   */
  JdbcTupleBatchIterator(final ResultSet resultSet, final Schema schema) {
    this.resultSet = resultSet;
    this.schema = schema;
  }

  @Override
  public boolean hasNext() {
    if (nextTB != null) {
      return true;
    } else {
      try {
        nextTB = getNextTB();
        return null != nextTB;
      } catch (final SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @return next TupleBatch, null if no more
   * @throws SQLException if any DB system errors
   * */
  private TupleBatch getNextTB() throws SQLException {
    if (statementClosed) {
      return null;
    }
    final int numFields = schema.numColumns();
    final List<ColumnBuilder<?>> columnBuilders = ColumnFactory.allocateColumns(schema);
    int numTuples = 0;
    for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE; ++numTuples) {
      if (!resultSet.next()) {
        final Connection connection = resultSet.getStatement().getConnection();
        resultSet.getStatement().close();
        connection.close(); /* Also closes the resultSet */
        statementClosed = true;
        break;
      }
      for (int colIdx = 0; colIdx < numFields; ++colIdx) {
        /* Warning: JDBC is 1-indexed */
        columnBuilders.get(colIdx).appendFromJdbc(resultSet, colIdx + 1);
      }
    }
    if (numTuples > 0) {
      List<Column<?>> columns = new ArrayList<Column<?>>(columnBuilders.size());
      for (ColumnBuilder<?> cb : columnBuilders) {
        columns.add(cb.build());
      }

      return new TupleBatch(schema, columns, numTuples);
    } else {
      return null;

    }
  }

  @Override
  public TupleBatch next() {
    TupleBatch tmp = nextTB;
    nextTB = null;
    return tmp;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("JdbcTupleBatchIterator.remove()");
  }
}