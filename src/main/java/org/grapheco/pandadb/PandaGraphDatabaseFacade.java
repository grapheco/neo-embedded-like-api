package org.grapheco.pandadb;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PandaGraphDatabaseFacade implements GraphDatabaseService {

    /**
     * Use this method to check if the database is currently in a usable state.
     *
     * @param timeout timeout (in milliseconds) to wait for the database to become available.
     *                If the database has been shut down {@code false} is returned immediately.
     * @return the state of the database: {@code true} if it is available, otherwise {@code false}
     */
    @Override
    public boolean isAvailable(long timeout) {
        return true;
    }

    /**
     * Starts a new {@link Transaction transaction} and associates it with the current thread.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside of a transaction, those operations will throw
     * {@link NotInTransactionException}.
     * <p>
     * Please ensure that any returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return a new transaction instance
     */
    @Override
    public Transaction beginTx() {
        return null;
    }

    /**
     * Starts a new {@link Transaction transaction} with custom timeout and associates it with the current thread.
     * Timeout will be taken into account <b>only</b> when execution guard is enabled.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside of a transaction, those operations will throw
     * {@link NotInTransactionException}.
     * <p>
     * Please ensure that any returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param timeout transaction timeout
     * @param unit    time unit of timeout argument
     * @return a new transaction instance
     */
    @Override
    public Transaction beginTx(long timeout, TimeUnit unit) {
        return null;
    }

    /**
     * Executes query in a separate transaction.
     * Capable to execute periodic commit queries.
     *
     * @param query The query to execute
     * @throws QueryExecutionException If the Query contains errors
     */
    @Override
    public void executeTransactionally(String query) throws QueryExecutionException {

    }

    /**
     * Executes query in a separate transaction.
     * Capable to execute periodic commit queries.
     *
     * @param query      The query to execute
     * @param parameters Parameters for the query
     * @throws QueryExecutionException If the Query contains errors
     */
    @Override
    public void executeTransactionally(String query, Map<String, Object> parameters) throws QueryExecutionException {

    }

    /**
     * Executes query in a separate transaction and allow to query result to be consumed by provided {@link ResultTransformer}.
     * Capable to execute periodic commit queries.
     *
     * @param query             The query to execute
     * @param parameters        Parameters for the query
     * @param resultTransformer Query results consumer
     * @throws QueryExecutionException If the query contains errors
     */
    @Override
    public <T> T executeTransactionally(String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer) throws QueryExecutionException {
        return null;
    }

    /**
     * Executes query in a separate transaction and allows query result to be consumed by provided {@link ResultTransformer}.
     * If query will not gonna be able to complete within provided timeout time interval it will be terminated.
     * <p>
     * Capable to execute periodic commit queries.
     *
     * @param query             The query to execute
     * @param parameters        Parameters for the query
     * @param resultTransformer Query results consumer
     * @param timeout           Maximum duration of underlying transaction
     * @throws QueryExecutionException If the query contains errors
     */
    @Override
    public <T> T executeTransactionally(String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer, Duration timeout) throws QueryExecutionException {
        return null;
    }

    /**
     * Return name of underlying database
     *
     * @return database name
     */
    @Override
    public String databaseName() {
        return null;
    }
}