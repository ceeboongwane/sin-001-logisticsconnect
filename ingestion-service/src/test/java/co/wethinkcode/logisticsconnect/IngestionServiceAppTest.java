package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import okhttp3.Response;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class IngestionServiceAppTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testCleanedRecordsEndpointPayloadStructure() {
        // Enforce application data normalization logic runs
        java.lang.reflect.Method[] methods = IngestionServiceApp.class.getDeclaredMethods();
        for (java.lang.reflect.Method m : methods) {
            if (m.getName().equals("processAndCleanCsv")) {
                try {
                    m.setAccessible(true);
                    m.invoke(null);
                } catch (Exception ignored) {}
            }
        }

        Javalin app = Javalin.create();
        app.get("/api/hubs", ctx -> {
            try {
                java.lang.reflect.Field field = IngestionServiceApp.class.getDeclaredField("cleanedHubs");
                field.setAccessible(true);
                Map<?, ?> map = (Map<?, ?>) field.get(null);
                ctx.json(map.values());
            } catch (Exception e) {
                ctx.status(500);
            }
        });

        JavalinTest.test(app, (server, client) -> {
            Response response = client.get("/api/hubs");
            assertEquals(200, response.code());

            List<?> records = mapper.readValue(response.body().string(), List.class);
            assertFalse(records.isEmpty(), "Ingested logs stack shouldn't return empty");

            // Inspect specific normalized values from your raw CSV mock rows
            for (Object obj : records) {
                Map<?, ?> hub = (Map<?, ?>) obj;
                String id = (String) hub.get("hubId");

                // Check standard identifier uppercase criteria
                assertEquals(id, id.toUpperCase());

                // Check specific boundary rows from your text file input data
                if ("H-501".equals(id)) {
                    assertEquals("Western Cape", hub.get("province"));
                    assertEquals("Cape Town Port", hub.get("sortingCenter")); // collapsed internal spaces
                    assertEquals(true, hub.get("active")); // resolved string yes -> boolean true
                }
                if ("H-508".equals(id)) {
                    assertNull(hub.get("province"), "Missing fields must resolve to clean JSON null tokens");
                }
                if ("H-517".equals(id)) {
                    assertNull(hub.get("active"), "N/A strings must resolve to null representations instead of crashing");
                }
            }
        });
    }
}
