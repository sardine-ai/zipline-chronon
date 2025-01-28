package ai.chronon.service.handlers;

import ai.chronon.online.*;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

// Configures the routes for our get features endpoints
// We support bulkGets of groupBys and bulkGets of joins
public class FetchRouter {

    public static class GroupByFetcherFunction implements BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> {
        @Override
        public CompletableFuture<List<JavaResponse>> apply(JavaFetcher fetcher, List<JavaRequest> requests) {
            return fetcher.fetchGroupBys(requests);
        }
    }

    public static class JoinFetcherFunction implements BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> {
        @Override
        public CompletableFuture<List<JavaResponse>> apply(JavaFetcher fetcher, List<JavaRequest> requests) {
            return fetcher.fetchJoin(requests);
        }
    }

    public static Router createFetchRoutes(Vertx vertx, Api api) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        JavaFetcher fetcher = api.buildJavaFetcher("feature-service", false);

        router.post("/groupby/:name").handler(new FetchHandler(fetcher, new GroupByFetcherFunction()));
        router.post("/join/:name").handler(new FetchHandler(fetcher, new JoinFetcherFunction()));

        return router;
    }
}
