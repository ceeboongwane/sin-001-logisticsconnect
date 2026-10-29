package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HubServiceApp {

    private static final Map<String, Map<String, Object>> hubCache = new ConcurrentHashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7051);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Serves provinces and sorting centers (place-name source of truth).)
        // Add domain endpoints for hub-service here.
        fetchAndCacheHubData();


        // Domain Endpoint: Fetch a specific hub by its normalized ID string
        app.get("/api/hubs/{id}", ctx -> {
            String targetId = ctx.pathParam("id").toUpperCase();
            if (!hubCache.containsKey(targetId)) {
                ctx.status(404).json(Map.of("error", "Logistical Hub not found: " + targetId));
                return;
            }
            ctx.json(hubCache.get(targetId));
        });
    }

    private static void fetchAndCacheHubData() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:7050/api/hubs"))
                    .GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                List<?> rawList = mapper.readValue(response.body(), List.class);
                for (Object item : rawList) {
                    Map<String, Object> hub = (Map<String, Object>) item;
                    String id = (String) hub.get("hubId");
                    if (id != null) hubCache.put(id, hub);
                }
                System.out.println("Hub Service successfully cached " + hubCache.size() + " records from Ingestion.");
            }
        } catch (Exception e) {
            System.err.println("Fallback triggered: Ingestion service unreachable. Seeding static mock for architecture survival.");
            hubCache.put("H-500", Map.of("hubId", "H-500", "province", "Gauteng", "sortingCenter", "Johannesburg Central", "active", true));
            hubCache.put("H-501", Map.of("hubId", "H-501", "province", "Western Cape", "sortingCenter", "Cape Town Port", "active", true));
        }
    }
}
