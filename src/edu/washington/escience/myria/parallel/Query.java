package edu.washington.escience.myria.parallel;

import java.util.LinkedList;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;

/**
 * Keeps track of all the state (statistics, subqueries, etc.) for a single Myria query.
 */
public final class Query {
  /** The id of this query. */
  private final long queryId;
  /** The id of the next subquery to be issued. */
  private long subqueryId;
  /** The status of the query. Must be kept synchronized. */
  @GuardedBy("this")
  private Status status;
  /** The subqueries to be executed. */
  private final LinkedList<SubQuery> subQueryQ;
  /** The query plan tasks to be executed. */
  private final LinkedList<QueryPlan> planQ;
  /** The execution statistics about this query. */
  private final ExecutionStatistics executionStats;
  /** The currently-executing subquery. */
  private SubQuery currentSubQuery;
  /** The server. */
  private final Server server;
  /** The message explaining why a query failed. */
  private String message;
  /** The future for this query. */
  private final QueryFuture future;
  /** True if the query should be run with profiling enabled. */
  private final boolean profiling;
  /** Indicates whether the query should be run with a particular fault tolerance mode. */
  private final FTMODE ftMode;

  /**
   * Construct a new {@link Query} object for this query.
   * 
   * @param queryId the id of this query
   * @param query contains the query options (profiling, fault tolerance)
   * @param plan the execution plan
   * @param server the server on which this query will be executed
   */
  public Query(final long queryId, final QueryEncoding query, final QueryPlan plan, final Server server) {
    Preconditions.checkNotNull(query, "query");
    this.server = Preconditions.checkNotNull(server, "server");
    profiling = Objects.firstNonNull(query.profilingMode, false);
    ftMode = Objects.firstNonNull(query.ftMode, FTMODE.none);
    this.queryId = queryId;
    subqueryId = 0;
    synchronized (this) {
      status = Status.ACCEPTED;
    }
    executionStats = new ExecutionStatistics();
    subQueryQ = new LinkedList<>();
    planQ = new LinkedList<>();
    planQ.add(plan);
    message = null;
    future = QueryFuture.create(queryId);
  }

  /**
   * Return the id of this query.
   * 
   * @return the id of this query
   */
  public long getQueryId() {
    return queryId;
  }

  /**
   * Return the status of this query.
   * 
   * @return the status of this query.
   */
  public QueryStatusEncoding.Status getStatus() {
    synchronized (this) {
      return status;
    }
  }

  /**
   * Returns <code>true</code> if this query has finished, and <code>false</code> otherwise.
   * 
   * @return <code>true</code> if this query has finished, and <code>false</code> otherwise
   */
  public boolean isDone() {
    return subQueryQ.isEmpty() && planQ.isEmpty();
  }

  /**
   * Returns the {@link SubQuery} that is currently executing, or <code>null</code> if nothing is running.
   * 
   * @return the {@link SubQuery} that is currently executing, or <code>null</code> if nothing is running
   */
  public SubQuery getCurrentSubQuery() {
    return currentSubQuery;
  }

  /**
   * Generates and returns the next {@link SubQuery} to run.
   * 
   * @return the next {@link SubQuery} to run
   * @throws DbException if there is an error
   */
  public SubQuery nextSubQuery() throws DbException {
    Preconditions.checkState(currentSubQuery == null, "must call finishSubQuery before calling nextSubQuery");
    if (isDone()) {
      return null;
    }
    if (!subQueryQ.isEmpty()) {
      currentSubQuery = subQueryQ.removeFirst();
      currentSubQuery.setSubQueryId(new SubQueryId(queryId, subqueryId));
      ++subqueryId;
      QueryConstruct.setQueryExecutionOptions(currentSubQuery.getWorkerPlans(), ftMode, profiling);
      return currentSubQuery;
    }
    planQ.getFirst().instantiate(planQ, subQueryQ, server);
    /*
     * The above line may have emptied planQ, mucked with subQueryQ, not sure. So just recurse to make sure we do the
     * right thing.
     */
    return nextSubQuery();
  }

  /**
   * Mark the current {@link SubQuery} as finished.
   */
  public void finishSubQuery() {
    currentSubQuery = null;
  }

  /**
   * Returns the time this query started, in ISO8601 format, or <code>null</code> if the query has not yet been started.
   * 
   * @return the time this query started, in ISO8601 format, or <code>null</code> if the query has not yet been started
   */
  public String getStartTime() {
    return executionStats.getStartTime();
  }

  /**
   * Set the time this query started to now in ISO8601 format.
   */
  public void markStart() {
    executionStats.markStart();
  }

  /**
   * Set this query as having failed due to the specified cause.
   * 
   * @param cause the reason the query failed.
   */
  public void markFailed(final Throwable cause) {
    Preconditions.checkNotNull(cause, "cause");
    markEnd();
    synchronized (this) {
      status = Status.ERROR;
    }
    message = cause.getMessage();
    future.setException(cause);
  }

  /**
   * Set the time this query ended to now in ISO8601 format.
   */
  public void markSuccess() {
    markEnd();
    synchronized (this) {
      status = Status.SUCCESS;
    }
    future.set(this);
  }

  /**
   * Set the time this query ended to now in ISO8601 format.
   */
  private void markEnd() {
    Verify.verify(currentSubQuery == null, "expect current subquery to be null when query ends");
    executionStats.markEnd();
  }

  /**
   * Returns the time this query ended, in ISO8601 format, or <code>null</code> if the query has not yet ended.
   * 
   * @return the time this query ended, in ISO8601 format, or <code>null</code> if the query has not yet ended
   */
  public String getEndTime() {
    return executionStats.getEndTime();
  }

  /**
   * Returns the time elapsed (in nanoseconds) since the query started.
   * 
   * @return the time elapsed (in nanoseconds) since the query started
   */
  public Long getElapsedTime() {
    return executionStats.getQueryExecutionElapse();
  }

  /**
   * Return a message explaining why a query failed, or <code>null</code> if the query did not fail.
   * 
   * @return a message explaining why a query failed, or <code>null</code> if the query did not fail
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns the future on the execution of this query.
   * 
   * @return the future on the execution of this query
   */
  public QueryFuture getFuture() {
    return future;
  }

  /**
   * Call when the query has been killed.
   */
  public void markKilled() {
    markEnd();
    synchronized (this) {
      status = Status.SUCCESS;
    }
    future.cancel(true);
  }

  /**
   * @return the fault tolerance mode for this query.
   */
  protected FTMODE getFtMode() {
    return ftMode;
  }

  /**
   * @return true if this query should be profiled.
   */
  protected boolean isProfilingMode() {
    return profiling;
  }
}