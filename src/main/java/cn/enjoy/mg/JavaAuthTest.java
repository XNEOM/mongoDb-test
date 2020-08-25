package cn.enjoy.mg;

import static com.mongodb.client.model.Filters.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


/**
 * @author wx
 * @date 20200825
 */

public class JavaAuthTest {

	private static final Logger logger = LoggerFactory
			.getLogger(JavaAuthTest.class);

	private MongoDatabase db;

	private MongoCollection<Document> collection;


	private MongoClient client;

	@Before
	public void init() {
		//MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
		MongoCredential createCredential =
				MongoCredential.createCredential("root", "admin", "123456".toCharArray());
		MongoClientOptions mco = MongoClientOptions.builder()
				.writeConcern(WriteConcern.JOURNALED)
				.connectionsPerHost(100)
				.readPreference(ReadPreference.secondary())
				.threadsAllowedToBlockForConnectionMultiplier(5)
				.maxWaitTime(120000).connectTimeout(10000).build();
		List<ServerAddress> asList = Arrays.asList(
				new ServerAddress("10.73.9.111",27017));
		this.client = new MongoClient(asList, createCredential,mco);
		db = client.getDatabase("admin");
		collection = db.getCollection("users");
	}

	// -----------------------------操作符使用实例------------------------------------------

	// db.users.find({"username":{"$in":["lison", "mark", "james"]}}).pretty()
	// 查询姓名为lison、mark和james这个范围的人
	@Test
	public void testInOper() {
		Bson in = in("username", "lison", "mark", "james");
		FindIterable<Document> find = collection.find(in);
		printOperation(find);
	}
	
	// ---------------------------------------------------------------------------

	//返回对象的处理器，打印每一行数据
	private Block<Document> getBlock(final List<Document> ret) {
		Block<Document> printBlock = new Block<Document>() {
			@Override
			public void apply(Document t) {
				logger.info("---------------------");
				logger.info(t.toJson());
				logger.info("---------------------");
				ret.add(t);
			}
		};
		return printBlock;
	}

	//打印查询出来的数据和查询的数据量
	private void printOperation( FindIterable<Document> find) {
		final List<Document> ret = new ArrayList<Document>();
		Block<Document> printBlock = getBlock(ret);
		find.forEach(printBlock);
		System.out.println(ret.size());
		ret.removeAll(ret);
	}
}