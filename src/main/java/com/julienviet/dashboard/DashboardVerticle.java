package com.julienviet.dashboard;

import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.rxjava.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.rxjava.kafka.client.consumer.KafkaConsumer;
import io.vertx.rxjava.kafka.client.consumer.KafkaConsumerRecord;
import rx.Observable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DashboardVerticle extends AbstractVerticle {

  @Override
  public void start() throws Exception {

    Router router = Router.router(vertx);

    // The event bus bridge handler
    BridgeOptions options = new BridgeOptions();
    options.setOutboundPermitted(Collections.singletonList(new PermittedOptions().setAddress("dashboard")));
    router.route("/eventbus/*").handler(SockJSHandler.create(vertx).bridge(options));

    // The web server handler
    router.route().handler(StaticHandler.create().setCachingEnabled(false));

    // Start http server
    HttpServer httpServer = vertx.createHttpServer();
    httpServer.requestHandler(router::accept).listen(8080, ar -> {
      if (ar.succeeded()) {
        System.out.println("Http server started");
      } else {
        ar.cause().printStackTrace();
      }
    });

    // Get the Kafka consumer config
    JsonObject config = config();

    // Create the consumer
    KafkaConsumer<String, JsonObject> consumer = KafkaConsumer.create(vertx, (Map) config.getMap(), String.class, JsonObject.class);
/*
    // Our dashboard that aggregates metrics from various kafka topics
    JsonObject dashboard = new JsonObject();

    // Publish the dashboard to the browser over the bus
    vertx.setPeriodic(1000, timerID -> {
      vertx.eventBus().publish("dashboard", dashboard);
    });

    // Aggregates metrics in the dashboard
    consumer.handler(record -> {
      JsonObject obj = record.value();
      dashboard.mergeIn(obj);
    });
*/

    Observable<KafkaConsumerRecord<String, JsonObject>> stream = consumer.toObservable();


    stream
        .map(record -> record.value())
        .buffer(1, TimeUnit.SECONDS)
        .map((List<JsonObject> metrics) -> {
          JsonObject dashboard = new JsonObject();
          for (JsonObject metric : metrics) {
            dashboard.mergeIn(metric);
          }
          return dashboard;
        }).subscribe(dashboard -> {
      vertx.eventBus().publish("dashboard", dashboard);
    });

    // Subscribe to Kafka
    consumer.subscribe(Collections.singleton("the_topic"));
  }
}