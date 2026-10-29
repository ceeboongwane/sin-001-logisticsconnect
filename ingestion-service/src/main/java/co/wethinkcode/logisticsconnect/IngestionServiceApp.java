package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class IngestionServiceApp {

    private static final Map<String, Hub> cleanedHubs = new LinkedHashMap<>();

    public static void main(String[] args) {

        processAndCleanCsv();

        Javalin app = Javalin.create().start(7050);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO: read and clean src/main/resources/hubs-global.csv (hubs, sorting centers, regional districts data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
        app.get("/api/hubs", ctx -> ctx.json(cleanedHubs.values()));
    }

    private static void processAndCleanCsv() {
        try (InputStream is = IngestionServiceApp.class.getResourceAsStream("/hubs-global.csv")) {
            if (is == null) {
                System.err.println("CRITICAL: src/main/resources/hubs-global.csv not found!");
                return;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                boolean isHeader = true;

                while ((line = reader.readLine()) != null) {
                    if (isHeader) {
                        isHeader = false; // Bypass columns definition row
                        continue;
                    }

                    // Split lines by comma while maintaining placeholders for empty trailing columns
                    String[] tokens = line.split(",", -1);
                    if (tokens.length < 4) continue;

                    // Clean and Normalize column fields
                    String rawHubId = tokens[0].trim().toUpperCase();
                    String rawProvince = tokens[1].trim();
                    String rawSortingCenter = tokens[2].trim();
                    String rawActive = tokens[3].trim();

                    // Skip processing if primary identifier is empty
                    if (rawHubId.isEmpty()) continue;

                    // Collapse double spaces down to single internal spacing boundaries
                    rawSortingCenter = rawSortingCenter.replaceAll("\\s+", " ");
                    if (!rawSortingCenter.isEmpty()) {
                        // Standardize sorting centers to Capitalized Title Case
                        rawSortingCenter = capitalizeTitleCase(rawSortingCenter);
                    }

                    // Normalize South African Province Names safely using formal standards
                    String cleanProvince = normalizeProvince(rawProvince);

                    // Resolve Booleans and Placeholder States cleanly
                    Boolean cleanActive = normalizeBoolean(rawActive);

                    // Assemble Domain Entity Object
                    Hub hub = new Hub(
                            rawHubId,
                            cleanProvince.isEmpty() ? null : cleanProvince,
                            rawSortingCenter.isEmpty() ? null : rawSortingCenter,
                            cleanActive
                    );

                    // Deduplication: Last-Write-Wins overwrites matching historical entries automatically
                    cleanedHubs.put(rawHubId, hub);
                }
                System.out.println("Data Engineering Job Completed successfully. Total unique hubs ingested: " + cleanedHubs.size());
            }
        } catch (Exception e) {
            System.err.println("Error running CSV processing job: " + e.getMessage());
        }
    }

    private static String normalizeProvince(String province) {
        String lower = province.toLowerCase().replaceAll("[^a-z]", "");
        if (lower.contains("gauteng")) return "Gauteng";
        if (lower.contains("westerncape")) return "Western Cape";
        if (lower.contains("kwazulunatal") || lower.contains("kwazulu")) return "KwaZulu-Natal";
        if (lower.contains("freestate")) return "Free State";
        if (lower.contains("easterncape")) return "Eastern Cape";
        if (lower.contains("limpopo")) return "Limpopo";
        if (lower.contains("northwest")) return "North West";
        if (lower.contains("mpumalanga")) return "Mpumalanga";
        if (lower.contains("northerncape")) return "Northern Cape";
        return province.trim(); // Return fallback if unmatched
    }

    private static Boolean normalizeBoolean(String value) {
        String clean = value.trim().toLowerCase();
        if (clean.equals("y") || clean.equals("yes") || clean.equals("1") || clean.equals("true")) {
            return true;
        }
        if (clean.equals("n") || clean.equals("no") || clean.equals("0") || clean.equals("false")) {
            return false;
        }
        return null; // For 'unknown', 'N/A' states, defaults to null token
    }

    private static String capitalizeTitleCase(String input) {
        StringBuilder result = new StringBuilder();
        String[] words = input.split(" ");
        for (String word : words) {
            if (word.length() > 0) {
                result.append(Character.toUpperCase(word.charAt(0)))
                        .append(word.substring(1).toLowerCase())
                        .append(" ");
            }
        }
        return result.toString().trim();
    }

    public static class Hub {
        public String hubId;
        public String province;
        public String sortingCenter;
        public Boolean active;

        public Hub(String hubId, String province, String sortingCenter, Boolean active) {
            this.hubId = hubId;
            this.province = province;
            this.sortingCenter = sortingCenter;
            this.active = active;
        }
    }
}
