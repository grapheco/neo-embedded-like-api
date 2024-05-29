package org.grapheco.pandadb;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.*;
import org.neo4j.values.virtual.MapValue;

import java.util.List;

public class PandaQueryExecutionEngine implements QueryExecutionEngine {

    public static PandaQueryExecutionEngine instance = new PandaQueryExecutionEngine();

    private PandaQueryExecutionEngine(){
    }

    @Override
    public Result executeQuery(String query, MapValue parameters, TransactionalContext context, boolean prePopulate) throws QueryExecutionKernelException {
        return null;
    }

    @Override
    public QueryExecution executeQuery(String query, MapValue parameters, TransactionalContext context, boolean prePopulate, QuerySubscriber subscriber) throws QueryExecutionKernelException {
        return null;
    }

    /**
     * @param query
     * @return {@code true} if the query is a PERIODIC COMMIT query and not an EXPLAIN query
     */
    @Override
    public boolean isPeriodicCommit(String query) {
        return false;
    }

    @Override
    public long clearQueryCaches() {
        return 0;
    }

    @Override
    public List<FunctionInformation> getProvidedLanguageFunctions() {
        return null;
    }
}
