import org.bson.Document;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.include;

import java.util.Arrays;

import static com.mongodb.client.model.Projections.fields;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

/**
 * Example on how to use the Mongo API directly.
 */
public class Assignment7 {
	/**
	 * Main method
	 * 
	 * @param args
	 *             no arg required
	 */
	public static void main(String[] args) {
		// Get MongoDB URI from environment variable
		String url = System.getenv("MONGODB_URI");
		if (url == null || url.isEmpty()) {
			System.err.println("Error: MONGODB_URI environment variable is not set.");
			System.exit(1);
		}

		try (
			// Provide connection information to MongoDB server
			MongoClient mongoClient = MongoClients.create(url);) {
			// Provide database information to connect to
			MongoDatabase db = mongoClient.getDatabase("tpch");

			// Load data
			loadCustomerData(db);
			loadOrdersData(db);

			// Run queries
			runQueries(db);

			System.out.println("Assignment completed successfully!");
		} catch (Exception ex) {
			System.out.println("Exception: " + ex);
			ex.printStackTrace();
		}
	}

	private static void loadCustomerData(MongoDatabase db) throws Exception {
		MongoCollection<Document> collection = db.getCollection("customer");
		collection.drop(); // Clear existing

		BufferedReader br = new BufferedReader(new FileReader("assignment/data/customer.tbl"));
		String line;
		List<WriteModel<Document>> batch = new ArrayList<>();
		int batchSize = 10000;
		while ((line = br.readLine()) != null) {
			String[] parts = line.split("\\|");
			Document doc = new Document()
					.append("c_custkey", Integer.parseInt(parts[0]))
					.append("c_name", parts[1])
					.append("c_address", parts[2])
					.append("c_nationkey", Integer.parseInt(parts[3]))
					.append("c_phone", parts[4])
					.append("c_acctbal", Double.parseDouble(parts[5]))
					.append("c_mktsegment", parts[6])
					.append("c_comment", parts[7]);
			batch.add(new InsertOneModel<>(doc));
			if (batch.size() >= batchSize) {
				collection.bulkWrite(batch, new BulkWriteOptions().ordered(false));
				batch.clear();
			}
		}
		if (!batch.isEmpty()) {
			collection.bulkWrite(batch, new BulkWriteOptions().ordered(false));
		}
		br.close();
		System.out.println("Customer data loaded.");
	}

	private static void loadOrdersData(MongoDatabase db) throws Exception {
		MongoCollection<Document> collection = db.getCollection("orders");
		collection.drop(); // Clear existing

		BufferedReader br = new BufferedReader(new FileReader("assignment/data/order.tbl"));
		String line;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<WriteModel<Document>> batch = new ArrayList<>();
		int batchSize = 10000;
		while ((line = br.readLine()) != null) {
			String[] parts = line.split("\\|");
			Date orderDate = sdf.parse(parts[4]);
			Document doc = new Document()
					.append("o_orderkey", Integer.parseInt(parts[0]))
					.append("o_custkey", Integer.parseInt(parts[1]))
					.append("o_orderstatus", parts[2])
					.append("o_totalprice", Double.parseDouble(parts[3]))
					.append("o_orderdate", orderDate)
					.append("o_orderpriority", parts[5])
					.append("o_clerk", parts[6])
					.append("o_shippriority", Integer.parseInt(parts[7]))
					.append("o_comment", parts[8]);
			batch.add(new InsertOneModel<>(doc));
			if (batch.size() >= batchSize) {
				collection.bulkWrite(batch, new BulkWriteOptions().ordered(false));
				batch.clear();
			}
		}
		if (!batch.isEmpty()) {
			collection.bulkWrite(batch, new BulkWriteOptions().ordered(false));
		}
		br.close();
		System.out.println("Orders data loaded.");
	}

	private static void runQueries(MongoDatabase db) {
		// Example query: Find customers with acctbal > 5000
		MongoCollection<Document> customerCol = db.getCollection("customer");
		MongoCursor<Document> cursor = customerCol.find(new Document("c_acctbal", new Document("$gt", 5000))).iterator();
		System.out.println("Customers with acctbal > 5000:");
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
		cursor.close();

		// Example query: Count orders per status
		MongoCollection<Document> ordersCol = db.getCollection("orders");
		List<Document> pipeline = Arrays.asList(
			new Document("$group", new Document("_id", "$o_orderstatus").append("count", new Document("$sum", 1)))
		);
		MongoCursor<Document> results = ordersCol.aggregate(pipeline).iterator();
		System.out.println("Order counts by status:");
		while (results.hasNext()) {
			System.out.println(results.next());
		}
		results.close();
	}
}
