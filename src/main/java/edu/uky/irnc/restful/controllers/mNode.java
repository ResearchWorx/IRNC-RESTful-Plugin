package edu.uky.irnc.restful.controllers;

import java.util.UUID;


public class mNode {

	  public String type;
	  public String name;
	  public String id;
	  public String commands;
	  public String qhost;
	  public String qport;
	  public String qlogin;
	  public String qpassword;
	  public String qname;
	  public int status_code;
	  public String status_desc;

	  public String location;

	  public mNode(String type, String name, String commands)
	  {
		  this.type = type;
		  this.name = name;
		  this.id = UUID.randomUUID().toString();
		  this.commands = commands;
		  this.status_code = 1;
		  this.status_desc = "Record Created";
	  }

	public mNode(String type, String name, String commands, String qhost, String qport, String qlogin, String qpassword, String qname)
	{
		this.type = type;
		this.name = name;
		this.id = UUID.randomUUID().toString();
		this.commands = commands;
		this.qhost = qhost;
		this.qlogin = qlogin;
		this.qport = qport;
		this.qpassword = qpassword;
		this.qname = qname;
		this.status_code = 1;
		this.status_desc = "Record Created";
	}

	}