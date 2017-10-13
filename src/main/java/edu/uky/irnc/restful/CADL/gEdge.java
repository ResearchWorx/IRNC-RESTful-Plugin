package edu.uky.irnc.restful.CADL;


public class gEdge {

	  public String edge_id;
	  public String node_from;
	  public String node_to;
	  
	  
	  public gEdge(String edge_id, String node_from, String node_to)
	  {
		 this.edge_id = edge_id;
		 this.node_from = node_from;
		 this.node_to = node_to;
	  }
	  
	}