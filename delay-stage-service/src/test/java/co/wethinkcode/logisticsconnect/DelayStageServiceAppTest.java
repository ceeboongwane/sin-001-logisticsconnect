package co.wethinkcode.logisticsconnect;

import co.wethinkcode.logisticsconnect.mq.MqConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import okhttp3.Response;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.jms.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DelayStageServiceAppTest {

    private static BrokerService embeddedBroker;
    private static ConnectionFactory testConnectionFactory;
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    public static void startEmbeddedBroker() throws Exception {
        // 1. Provision localized internal ActiveMQ Broker instance
        embeddedBroker = new BrokerService();
        embeddedBroker.setPersistent(false);
        embeddedBroker.setUseJmx(false);
        embeddedBroker.addConnector(MqConfig.BROKER_URL);
        embeddedBroker.start();

        testConnectionFactory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
    }

    @AfterAll
    public static void stopEmbeddedBroker() throws Exception {
        if (embeddedBroker != null) {
            embeddedBroker.stop();
        }
    }

    /**
     * Factory utility to generate isolated application test loops
     * without port collision risks on the 7052 pipeline.
     */
    private Javalin createFreshAppInstance() {
        // Inject connection factory manually for test runtime context separation
        try {
            java.lang.reflect.Field field = DelayStageServiceApp.class.getDeclaredField("connectionFactory");
            field.setAccessible(true);
            field.set(null, testConnectionFactory);
        } catch (Exception e) {
            fail("Failed framework setup: " + e.getMessage());
        }

        Javalin app = Javalin.create();
        app.get("/api/delays/{province}", DelayStageServiceApp::getProvinceDelay);
        app.post("/api/delays/{province}", DelayStageServiceApp::updateProvinceDelay);
        return app;
    }

    @Test
    public void testUpdateProvinceDelayValidBounds() {
        JavalinTest.test(createFreshAppInstance(), (server, client) -> {
            String jsonPayload = mapper.writeValueAsString(Map.of("transitDelayStage", 5));
            Response response = client.post("/api/delays/Gauteng", jsonPayload);

            assertEquals(200, response.code());
            String body = response.body().string();
            assertTrue(body.contains("Transit Delay Stage updated successfully"));
            assertTrue(body.contains("GAUTENG"));

            // Verify the inner cache modified cleanly via a side GET verification check
            Response getResponse = client.get("/api/delays/Gauteng");
            Map<?, ?> getBody = mapper.readValue(getResponse.body().string(), Map.class);
            assertEquals(5, ((Number) getBody.get("transitDelayStage")).intValue());
        });
    }

    @Test
    public void testUpdateProvinceDelayExceedsUpperBound() {
        JavalinTest.test(createFreshAppInstance(), (server, client) -> {
            String jsonPayload = mapper.writeValueAsString(Map.of("transitDelayStage", 9));
            Response response = client.post("/api/delays/Limpopo", jsonPayload);

            assertEquals(400, response.code());
            String body = response.body().string();
            assertTrue(body.contains("Transit Delay Stage must be an integer between 0 and 8 inclusive"));
        });
    }

    @Test
    public void testUpdateProvinceDelayPublishesAsynchronousEvent() throws Exception {
        // 1. Arrange: Use a unique client ID to create an isolated subscriber session
        try (Connection connection = testConnectionFactory.createConnection()) {
            // Enforces broker message stream isolation for this exact thread slice
            connection.setClientID("delay-stage-test-subscriber");
            connection.start();

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = session.createTopic(MqConfig.TOPIC);
                try (MessageConsumer topicConsumer = session.createConsumer(topic)) {

                    // 2. Act: Send the target validation update post request
                    JavalinTest.test(createFreshAppInstance(), (server, client) -> {
                        String jsonPayload = mapper.writeValueAsString(Map.of("transitDelayStage", 3));
                        Response response = client.post("/api/delays/KwaZulu-Natal", jsonPayload);
                        assertEquals(200, response.code());
                    });

                    // 3. Assert: Loop through messages to handle any leftover packets from previous tests
                    Message receivedEvent = null;
                    long timeout = System.currentTimeMillis() + 1500;

                    while (System.currentTimeMillis() < timeout) {
                        Message msg = topicConsumer.receive(500);
                        if (msg instanceof TextMessage) {
                            String txt = ((TextMessage) msg).getText();
                            // Ignore leftover data packets from other test methods
                            if (txt.contains("KWAZULU-NATAL")) {
                                receivedEvent = msg;
                                break;
                            }
                        }
                    }

                    assertNotNull(receivedEvent, "The KwaZulu-Natal update notification must be broadcast onto the topic!");
                    String rawJson = ((TextMessage) receivedEvent).getText();
                    Map<?, ?> payload = mapper.readValue(rawJson, Map.class);

                    assertEquals("KWAZULU-NATAL", payload.get("province"));
                    assertEquals(3, ((Number) payload.get("transitDelayStage")).intValue());
                    assertNotNull(payload.get("timestamp"));
                }
            }
        }
    }
}
