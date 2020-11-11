package uk.ac.ebi.biostudies.studies2mongo;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Studies2Mongo {

    public static int THREADS = 10;
    public static int QUEUE_SIZE = 100;
    public static AtomicInteger ActiveExecutorService = new AtomicInteger(0);
    public static ExecutorService executorService = new ThreadPoolExecutor(THREADS, THREADS, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(QUEUE_SIZE), new ThreadPoolExecutor.CallerRunsPolicy());
    public static MongoCollection<Document> collection;

    public static void main(String[] args) {

        if (args.length!=4) {
            System.out.println("Usage: Studies2Mongo <studies.json path> <mongo connection string> <mongo db name> <mongo collection name>");
            System.exit(0);
        }
        String studiesJsonFilePath = args[0], mongoConnectionString = args[1], mongoDbName = args[2], mongoCollectionName = args[3];
        long startTime = System.currentTimeMillis();
        MongoClient mongoClient = MongoClients.create(mongoConnectionString);
        MongoDatabase db = mongoClient.getDatabase(mongoDbName);
        collection = db.getCollection(mongoCollectionName);
        collection.createIndex(BsonDocument.parse("{\"accNo\":1}"), new IndexOptions().unique(true) );
        ActiveExecutorService.incrementAndGet();

        int counter = 0;
        try (InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(studiesJsonFilePath), StandardCharsets.UTF_8)) {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createParser(inputStreamReader);
            JsonToken token = parser.nextToken();
            while (!JsonToken.START_ARRAY.equals(token)) {
                token = parser.nextToken();
            }
            ObjectMapper mapper = new ObjectMapper();
            while (true) {
                token = parser.nextToken();
                if (!JsonToken.START_OBJECT.equals(token)) {
                    break;
                }
                if (token == null) {
                    break;
                }
                JsonNode submission = mapper.readTree(parser);
                updateSubmission(submission);
                if (++counter % 10000 == 0) {
                   System.out.println (counter + " docs indexed");
                }
            }
            Map<String, String> commitData = new HashMap<>();
            while (token != JsonToken.END_OBJECT) {
                if (token.name().equalsIgnoreCase("field_name")) {
                    String key = parser.getText();
                    token = parser.nextToken();
                    commitData.put(key, token.isNumeric() ? Long.toString(parser.getLongValue()) : parser.getText());
                }
                token = parser.nextToken();
            }

            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.HOURS);
            ActiveExecutorService.decrementAndGet();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Indexing time in  seconds: " + (System.currentTimeMillis()-startTime)/1000);
        }
    }

    private static void updateSubmission(JsonNode submission) {
        executorService.execute(new JsonDocumentIndexer(submission));
    }


    public static class JsonDocumentIndexer implements Runnable {

        private final JsonNode json;

        public JsonDocumentIndexer(JsonNode json) {
            this.json = json;
        }

        @Override
        public void run(){
            final boolean isInOldFormat = json.has("accno");
            String accession = isInOldFormat ? json.get("accno").textValue() : json.get("accNo").textValue();
            try {
                Document doc = Document.parse(json.toString());
                if (isInOldFormat) doc.put("accNo", accession);
                Studies2Mongo.collection.replaceOne (Filters.eq("accNo", accession), doc, new ReplaceOptions().upsert(true));
            } catch (Exception e) {
                System.err.println("error in " + accession);
                e.printStackTrace();
            }

        }

    }
}