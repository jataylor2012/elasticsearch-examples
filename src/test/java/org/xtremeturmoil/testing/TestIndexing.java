package org.xtremeturmoil.testing;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtremeturmoil.examples.FastBulkIndexing;
import org.xtremeturmoil.examples.NodeManager;

/**
 * This test demonstrates ingesting data into Elasticsearch with some
 * useful helpers for efficent bulk indexing. Changing the settings
 * below will alter the indexing speed. Please note that larger batch
 * sizes and buffers will require larger JVM max heap. Please also set
 * the minimum heap to the same as the max to reduce garbage collections.
 * 
 * Large Example settings -Xmx24G -Xms24G BATCH_SIZE=2000000 THREADCOUNT = 4
 * TEST_AMOUNT = 24000000 BUFFER_IN_MB = 128
 * @author JayTee
 *
 */
public class TestIndexing {

	private final int TEST_AMOUNT = 50000;
	private final int BATCH_SIZE = 10000;
	private final int THREAD_COUNT = 2;
	private final int FLUSH_TIME = 10;
	private final int BUFFER_IN_MB = 32;
	private final String TEST_INDEX = "testindex";
	private final String TEST_TYPE = "testdocument";

	@Before
	public void setUp() throws Exception {
		// Clear up and previous data.
		clearLocalData();
		// Setup a local test Elasticsearch.
		NodeManager.getInstance().startNode(true, false);
		FastBulkIndexing.getInstance().initialize(BATCH_SIZE, THREAD_COUNT, FLUSH_TIME, BUFFER_IN_MB);
	}

	@After
	public void tearDown() throws Exception {
		// Shutdown the local test Elasticsearch.
		NodeManager.getInstance().stopNode();
		// Clear up any local data.
		clearLocalData();
	}

	@Test
	public void testIndexing() {

		try {

			long startTime = System.currentTimeMillis();
			for(int i=0; i < TEST_AMOUNT; i++) {
				XContentBuilder document = XContentFactory.jsonBuilder()
						.startObject()
						.field("title","This is my title " + i)
						.field("timestamp", new Date())
						.field("content", "This is my content " + i)
						.endObject();

				FastBulkIndexing.getInstance().load(document, TEST_INDEX, TEST_TYPE);
			}
			
			// Wait until asynchronous loader has processed everything.
			while(FastBulkIndexing.getInstance().getProcessedCount() < TEST_AMOUNT) {
				Thread.sleep(1000);
			}
			System.out.println("Processed " + FastBulkIndexing.getInstance().getProcessedCount());
			
			long endTime = System.currentTimeMillis();
			long total = endTime-startTime;
			
			System.out.println("Indexed " + TEST_AMOUNT + " in " + total + "ms");
			long perSecond = TEST_AMOUNT / (total/1000);
			System.out.println("Documents per second: " + perSecond);
			// Wait until index refresh has occurred.
			long indexCount = 0;
			while((indexCount=getAmountInIndex()) != TEST_AMOUNT) {
				Thread.sleep(1000);
				System.out.println("Waiting for index refresh, " + indexCount);
			}
			
			assertTrue("Index contains " + getAmountInIndex(), getAmountInIndex() == TEST_AMOUNT);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail("Failure due to exception. " + e.getMessage());
		}


	}
	
	private long getAmountInIndex() {
		Client client = NodeManager.getInstance().getClient();
		return client.prepareCount(TEST_INDEX).setTypes(TEST_TYPE).get().getCount();
	}
	
	private void clearLocalData() {
		FileUtils.deleteQuietly(new File("./data"));
	}

}
