package co.wethinkcode.logisticsconnect;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class HubServiceAppTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private Javalin mockIngestionService;

    @AfterEach
    public void teardownMocks() {
        if (mockIngestionService != null) {
            mockIngestionService.stop();
        }
        // Safely wipe static state data cache map clean between test executions
        try {
            Field cacheField = HubServiceApp.class.getDeclaredField("hubCache");
            cacheField.setAccessible(true);
            Map<?, ?> map = (Map<?, ?>) cacheField.get(null);
            map.clear();
        } catch (Exception ignored) {}
    }

    /**
     * Reflective lifecycle helper to trigger data cache synchronization
     * on demand within our test scope.
     */
    private void triggerCacheSync() {
        try {
            Method syncMethod = HubServiceApp.class.getDeclaredMethod("fetchAndCacheHubData");
            syncMethod.setAccessible(true);
            syncMethod.invoke(null);
        } catch (Exception e) {
            fail("Failed initializing cache sync loop helper: " + e.getMessage());
        }
    }

    private Javalin createFreshAppInstance() {
        Javalin app = Javalin.create();
        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/api/hubs/{id}", ctx -> {
            // Re-bind identical lambda runtime logic to isolate under ephemeral port execution paths
            try {
                Field cacheField = HubServiceApp.class.getDeclaredField("hubCache");
                cacheField.setAccessible(true);
                Map<String, Map<String, Object>> cache = (Map<String, Map<String, Object>>) cacheField.get(null);

                String targetId = ctx.pathParam("id").toUpperCase();
                if (!cache.containsKey(targetId)) {
                    ctx.status(404).json(Map.of("error", "Logistical Hub not found: " + targetId));
                    return;
                }
                ctx.json(cache.get(targetId));
            } catch (Exception e) {
                ctx.status(500);
            }
        });
        return app;
    }

    @Test
    public void testFetchAndCacheHubDataSuccessPath() throws Exception {
        // Arrange: Spin up an active downstream mock environment on expected port 7050
        mockIngestionService = Javalin.create().start(7050);
        mockIngestionService.get("/api/hubs", ctx -> ctx.json(List.of(
                Map.of("hubId", "H-999", "province", "Mpumalanga", "sortingCenter", "Nelspruit Hub", "active", true)
        )));

        // Act: Sync state
        triggerCacheSync();

        // Assert: Endpoint yields successful payload
        JavalinTest.test(createFreshAppInstance(), (server, client) -> {
            Response response = client.get("/api/hubs/H-999");
            assertEquals(200, response.code());

            Map<?, ?> body = mapper.readValue(response.body().string(), Map.class);
            assertEquals("H-999", body.get("hubId"));
            assertEquals("Mpumalanga", body.get("province"));
            assertEquals("Nelspruit Hub", body.get("sortingCenter"));
        });
    }

    @Test
    public void testFetchAndCacheHubDataFallbackOnDependencyOutage() throws Exception {
        // Arrange: Ingestion is entirely offline (do not start server), forcing connection exceptions
        // Act: Trigger sync to execute catch fallback block
        triggerCacheSync();

        // Assert: App handles failure cleanly by exposing static fail-safes
        JavalinTest.test(createFreshAppInstance(), (server, client) -> {
            Response responseGauteng = client.get("/api/hubs/H-500");
            assertEquals(200, responseGauteng.code());
            Map<?, ?> bodyGauteng = mapper.readValue(responseGauteng.body().string(), Map.class);
            assertEquals("Gauteng", bodyGauteng.get("province"));

            Response responseWC = client.get("/api/hubs/H-501");
            assertEquals(200, responseWC.code());
            Map<?, ?> bodyWC = mapper.readValue(responseWC.body().string(), Map.class);
            assertEquals("Western Cape", bodyWC.get("province"));
        });
    }

    @Test
    public void testGetHubNotFoundSemantics() {
        // Act & Assert: Querying a completely non-existent item triggers valid 404 response
        JavalinTest.test(createFreshAppInstance(), (server, client) -> {
            Response response = client.get("/api/hubs/H-UNKNOWN");
            assertEquals(404, response.code());
            assertTrue(response.body().string().contains("Logistical Hub not found"));
        });
    }
}

