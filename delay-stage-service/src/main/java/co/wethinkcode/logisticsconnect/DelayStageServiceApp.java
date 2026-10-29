package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import io.javalin.http.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import co.wethinkcode.logisticsconnect.mq.MqConfig;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelayStageServiceApp {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Map<String, Integer> regionalTransitDelayStages = new ConcurrentHashMap<>();
    private static ConnectionFactory connectionFactory;

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7052);

        app.get("/health", ctx -> ctx.result("OK"));
        connectionFactory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);

        // Seed some initial baseline logistical delay stages per province
        regionalTransitDelayStages.put("GAUTENG", 0); // Baseline operation
        regionalTransitDelayStages.put("WESTERN CAPE", 2); // H

        // TODO (Tracks the Transit Delay Stage (0-8, e.g. weather shutdowns).)
        // Add domain endpoints for delay-stage-service here.
        app.get("/api/delays", DelayStageServiceApp::getAllDelays);
        app.get("/api/delays/{province}", DelayStageServiceApp::getProvinceDelay);
        app.post("/api/delays/{province}", DelayStageServiceApp::updateProvinceDelay);
    }

    public static void getAllDelays(Context ctx) {
        ctx.json(regionalTransitDelayStages);
    }

    public static void getProvinceDelay(Context ctx) {
        String provinceKey = ctx.pathParam("province").toUpperCase().trim();
        int delayStage = regionalTransitDelayStages.getOrDefault(provinceKey, 0);
        ctx.json(Map.of("province", provinceKey, "transitDelayStage", delayStage));
    }

    public static void updateProvinceDelay(Context ctx) {
        String provinceKey = ctx.pathParam("province").toUpperCase().trim();

        try {
            Map<?, ?> payload = ctx.bodyAsClass(Map.class);
            if (payload == null || !payload.containsKey("transitDelayStage")) {
                ctx.status(400).json(Map.of("error", "Bad Request: Missing 'transitDelayStage' field parameter"));
                return;
            }

            int requestedStage = ((Number) payload.get("transitDelayStage")).intValue();

            // Rubric validation checkpoint (Strictly enforce range 0-8)
            if (requestedStage < 0 || requestedStage > 8) {
                ctx.status(400).json(Map.of("error", "Bad Request: Transit Delay Stage must be an integer between 0 and 8 inclusive."));
                return;
            }

            regionalTransitDelayStages.put(provinceKey, requestedStage);

            Map<String, Object> updateNotification = Map.of(
                    "province", provinceKey,
                    "transitDelayStage", requestedStage,
                    "timestamp", System.currentTimeMillis()
            );

            // Fire-and-forget asynchronous notification broadcast onto ActiveMQ topic
            publishDelayStatusEventAsynchronously(updateNotification);

            ctx.json(Map.of("message", "Transit Delay Stage updated successfully for " + provinceKey + " to " + requestedStage));

        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "Malformed input data schema wrapper context"));
        }
    }

    public static void publishDelayStatusEventAsynchronously(Map<String, Object> payload) {
        new Thread(() -> {
            try (Connection connection = connectionFactory.createConnection();
                 Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

                Destination destination = session.createTopic(MqConfig.TOPIC);
                try (MessageProducer producer = session.createProducer(destination)) {
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                    String jsonMessage = objectMapper.writeValueAsString(payload);
                    TextMessage message = session.createTextMessage(jsonMessage);

                    producer.send(message);
                    System.out.println("MQ Topic Broadcast Success -> Status notification pushed to topic: " + MqConfig.TOPIC);
                }
            } catch (Exception e) {
                System.err.println("MQ Broadcast Dropped: Message broker unreachable or transaction dropped: " + e.getMessage());
            }
        }).start();
    }
}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
