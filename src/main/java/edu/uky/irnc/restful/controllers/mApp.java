package edu.uky.irnc.restful.controllers;

import edu.uky.irnc.restful.CADL.gEdge;
import edu.uky.irnc.restful.CADL.gNode;

import java.util.List;


public class mApp {

	  public String id;
	  public String name;
	  public int status_code;
	  public String status_desc;

	  public List<mNode> nodes;

	  public mApp(List<mNode> nodes)
	  {
		  this.nodes = nodes;
		  this.status_code = 1;
		  this.status_desc = "Record Created";
	  }
	  public mApp()
	  {
		  
	  }
	  
	}