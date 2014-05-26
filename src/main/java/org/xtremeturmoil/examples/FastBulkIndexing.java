package org.xtremeturmoil.examples;

import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * The Elasticsearch documentation is not obvious to interpret when it
 * comes to how to perform fast bulk indexing. In fact the documentation
 * does not mention that there are many helper classes that make faster
 * indexing significantly easier. This class shows how to use the 
 * {@link BulkProcessor} which given enough RAM/CPU can achieve
 * significantly faster indexing.
 * 
 * This example has been made as a singleton to allow for simple reuse.
 * @author JayTee
 *
 */
public class FastBulkIndexing {
	
	private static FastBulkIndexing instance;
	private AtomicInteger processed = new AtomicInteger();
	private BulkProcessor loader;

	private FastBulkIndexing() {}
	
	public static synchronized FastBulkIndexing getInstance() {
		if(instance==null) {
			instance = new FastBulkIndexing();
		}
		return instance;
	}
	
	public void initialize(int batchSize, int threadCount, int flushInSeconds, int bufferInMB) {
		Client client = NodeManager.getInstance().getClient();
		loader = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			
			public void beforeBulk(long arg0, BulkRequest arg1) {
				System.out.println("Processing " + arg1.numberOfActions());
			}
			
			public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {
				processed.addAndGet(arg1.numberOfActions());
				System.err.println("Failed to process " + arg1.numberOfActions() + " due to " + arg2.getMessage());
				arg2.printStackTrace();
			}
			
			public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {
				processed.addAndGet(arg1.numberOfActions());
				System.out.println("Processed " + arg1.numberOfActions());
				if(arg2.hasFailures()) {
					System.err.println("There were failures. " + arg2.buildFailureMessage());
				}
			}
		})
		.setBulkActions(batchSize)
		.setConcurrentRequests(threadCount)
		.setFlushInterval(TimeValue.timeValueSeconds(flushInSeconds))
		.setBulkSize(new ByteSizeValue(bufferInMB, ByteSizeUnit.MB))
		.build();
	}
	
	public int getProcessedCount() {
		return processed.get();
	}
	
	public void load(String jsonString, String index, String type) {
		IndexRequest toIndex = new IndexRequest(index, type).source(jsonString);
		loader.add(toIndex);
	}
	
	public void load(XContentBuilder document, String index, String type) {
		IndexRequest toIndex = new IndexRequest(index, type).source(document);
		loader.add(toIndex);
	}
}
