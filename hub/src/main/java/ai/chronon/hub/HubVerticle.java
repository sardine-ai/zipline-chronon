package ai.chronon.hub;

import ai.chronon.api.Constants;
import ai.chronon.hub.handlers.*;
import ai.chronon.hub.store.MonitoringModelStore;
import ai.chronon.online.Api;
import ai.chronon.online.KVStore;
import ai.chronon.online.stats.DriftStore;
import ai.chronon.service.ApiProvider;
import ai.chronon.service.ConfigStore;
import ai.chronon.spark.utils.InMemoryKvStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Chronon Hub HTTP service. We wire up our API routes and configure and launch the service here.
 * We choose to use just 1 verticle for now as it allows us to keep things simple and we don't need to scale /
 * independently deploy different endpoint routes.
 */
public class HubVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(HubVerticle.class);

    private HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        ConfigStore cfgStore = new ConfigStore(vertx);
        startHttpServer(cfgStore.getServerPort(), cfgStore.encodeConfig(), ApiProvider.buildApi(cfgStore), startPromise);
    }

    protected void startHttpServer(int port, String configJsonString, Api api, Promise<Void> startPromise) throws Exception {
        Router router = Router.router(vertx);
        wireUpCORSConfig(router);
        
        // Define routes

        // Health check route
        router.get("/ping").handler(ctx -> {
            ctx.json("Pong!");
        });

        // Add route to show current configuration
        router.get("/config").handler(ctx -> {
            ctx.response()
                    .putHeader("content-type", "application/json")
                    .end(configJsonString);
        });

        // Add routes for metadata retrieval
        MonitoringModelStore store = new MonitoringModelStore(api);
        router.get("/api/v1/models").handler(new ModelsHandler(store));
        router.get("/api/v1/join/:name").handler(new JoinsHandler(store).getHandler());
        router.get("/api/v1/joins").handler(new JoinsHandler(store).listHandler());
        router.get("/api/v1/search").handler(new SearchHandler(store));

        // hacked up in mem kv store bulkPut
        KVStore inMemoryKVStore = InMemoryKvStore.build("hub", () -> null);
        // create relevant datasets in kv store
        inMemoryKVStore.create(Constants.MetadataDataset());
        inMemoryKVStore.create(Constants.TiledSummaryDataset());
        router.route().handler(BodyHandler.create());
        router.post("/api/v1/dataset/data").handler(new InMemKVStoreHandler(inMemoryKVStore));

        // time series endpoints
        DriftStore driftStore = new DriftStore(inMemoryKVStore, Constants.TiledSummaryDataset(), Constants.MetadataDataset());
        router.get("/api/v1/join/:name/timeseries").handler(new TimeSeriesHandler(driftStore).joinDriftHandler());
        router.get("/api/v1/join/:join/feature/:name/timeseries").handler(new TimeSeriesHandler(driftStore).featureDriftHandler());

        // Start HTTP server
        HttpServerOptions httpOptions =
                new HttpServerOptions()
                        .setTcpKeepAlive(true)
                        .setIdleTimeout(60);
        server = vertx.createHttpServer(httpOptions);
        server.requestHandler(router)
                .listen(port)
                .onSuccess(server -> {
                    logger.info("HTTP server started on port {}", server.actualPort());
                    startPromise.complete();
                })
                .onFailure(err -> {
                    logger.error("Failed to start HTTP server", err);
                    startPromise.fail(err);
                });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        logger.info("Stopping HTTP server...");
        if (server != null) {
            server.close()
                    .onSuccess(v -> {
                        logger.info("HTTP server stopped successfully");
                        stopPromise.complete();
                    })
                    .onFailure(err -> {
                        logger.error("Failed to stop HTTP server", err);
                        stopPromise.fail(err);
                    });
        } else {
            stopPromise.complete();
        }
    }

    private void wireUpCORSConfig(Router router) {
        router.route().handler(CorsHandler.create()
                .addOrigin("http://localhost:5173")
                .addOrigin("http://localhost:3000")
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.POST)
                .allowedMethod(HttpMethod.PUT)
                .allowedMethod(HttpMethod.DELETE)
                .allowedMethod(HttpMethod.OPTIONS)
                .allowedHeader("Accept")
                .allowedHeader("Content-Type")
                .allowCredentials(false)); // Change to true if credentials are required
    }
}
