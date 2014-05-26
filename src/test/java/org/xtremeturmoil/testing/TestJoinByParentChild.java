package org.xtremeturmoil.testing;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtremeturmoil.examples.NodeManager;

/**
 * This is an example of how to perform a join in Elasticsearch using
 * its parent/child functionality.
 * @author JayTee
 *
 */
public class TestJoinByParentChild {

	@Before
	public void setUp() throws Exception {
		// Clear up and previous data.
		clearLocalData();
		// Setup a local test Elasticsearch.
		NodeManager.getInstance().startNode(true, false);
	}

	@After
	public void tearDown() throws Exception {
		// Shutdown the local test Elasticsearch.
		NodeManager.getInstance().stopNode();
		// Clear up any local data.
		clearLocalData();
	}

	/**
	 * Example join for two separate datasets. This example is
	 * for immigration and passport records.
	 */
	@Test
	public void testParentChild() {
		Client client = NodeManager.getInstance().getClient();
		try {
			
			// First we create some passport records. Kate is french, everyone else is english.
			List<String> people = Arrays.asList(new String[]{"Kate","Lucy","Bob","Jim","James"});
			int passportNumber = 0;
			for(String person : people) {
				XContentBuilder passportRecord = XContentFactory.jsonBuilder().startObject()
						.field("name", person)
						.field("passportnumber", passportNumber)
						.field("nationality", person.equals("Kate") ? "french" : "english")
						.endObject();

				// Force a synchronous put index for testing purposes.
				client.prepareIndex("data", "passport").setId(String.valueOf(passportNumber++)).setSource(passportRecord).setRefresh(true).get();
			}
			
			// Now create the join schema.
			putParentMapping("passport", "immigration");

			// Now lets create some immigration details with only passport numbers.
			for(int id =0; id < people.size(); id++) {
				XContentBuilder immigration = XContentFactory.jsonBuilder().startObject()
						.field("id",id)
						.field("timestamp", new Date())
						.field("airport", "LHR")
						.endObject();

				// Force a synchronous put index for testing purposes.
				client.prepareIndex("data", "immigration").setSource(immigration).setParent(String.valueOf(id)).setRefresh(true).get();
			}

			// Note that the immigration details join to the passport records using the primary key of the passport records.
			// Now we can query and find only french immigration for example...
			QueryBuilder query = QueryBuilders.hasParentQuery("passport", QueryBuilders.queryString("nationality:french"));
			SearchResponse frenchImmigration = client.prepareSearch("data").setQuery(query).get();

			System.out.println(frenchImmigration.getHits().getTotalHits() + " of immigration records were french.");
			assertTrue(frenchImmigration.getHits().getTotalHits() == 1);

			// Or query and find only english immigration...
			query = QueryBuilders.hasParentQuery("passport", QueryBuilders.queryString("nationality:english"));
			SearchResponse englishImmigration = client.prepareSearch("data").setQuery(query).get();

			System.out.println(englishImmigration.getHits().getTotalHits() + " of immigration records were english.");
			assertTrue(englishImmigration.getHits().getTotalHits() == 4);

			// Please note that the join is performed by elasticsearch in memory, therefore all passport documents should fit in memory and hence parent child
			// is only feasible for reference data sets that are small like passport details etc.


		} catch (Exception e) {
			e.printStackTrace();
			fail("Failure due to exception. " + e.getMessage());
		}

	}
	
	
	private void putParentMapping(String parentType, String childType) throws IOException {
		
		Client client = NodeManager.getInstance().getClient();
		
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(childType)
                        .startObject("_parent")
                            .field("type", parentType)
                        .endObject()
                    .endObject()
                .endObject();

        client.admin().indices().preparePutMapping("data")
                .setType(childType).setSource(mappingBuilder)
                .execute().actionGet();
    }

	private void clearLocalData() {
		FileUtils.deleteQuietly(new File("./data"));
	}

}
