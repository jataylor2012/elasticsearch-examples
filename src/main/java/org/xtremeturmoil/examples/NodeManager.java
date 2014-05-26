package org.xtremeturmoil.examples;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class NodeManager {

	private static NodeManager instance;
	private Node node;
	private Client client;
	
	private NodeManager() {}
	
	public static NodeManager getInstance() {
		if(instance == null) {
			instance = new NodeManager();
		}
		return instance;
	}
	
	/**
	 * Any settings should be set in elasticsearch.yml which can be found on
	 * the classpath or src/test/resources when testing.
	 * @param isTesting
	 * @param isClient
	 */
	public void startNode(boolean isTesting, boolean isClient) {
		node = NodeBuilder.nodeBuilder().local(isTesting).client(isClient).node();
		client = node.client();
	}
	
	public Client getClient() {
		return client;
	}
	
	public void stopNode() {
		if(client!=null) {
			client.close();
		}
		
		if(node!=null) {
			node.close();
		}
	}
	
}
