package edu.uky.irnc.restful.controllers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import edu.uky.irnc.restful.CADL.gEdge;
import edu.uky.irnc.restful.CADL.gNode;
import edu.uky.irnc.restful.CADL.gPayload;
import edu.uky.irnc.restful.Pipeline;
import edu.uky.irnc.restful.Plugin;
import edu.uky.irnc.restful.pipelineStatus;
import org.apache.commons.lang.StringEscapeUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unused")
@Path("API")
public class APIController {

    public static ConcurrentHashMap<String, MsgEvent> activePlugins = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, QueueListener> listeners = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, KanonWatcher> watchers = new ConcurrentHashMap<>();
    private static Plugin plugin;
    private static CLogger logger;
    private static String amqp_server;
    private static String amqp_port;
    private static String amqp_login;
    private static String amqp_password;

    public static void setPlugin(Plugin mainPlugin) {
        plugin = mainPlugin;
        logger = new CLogger(APIController.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                plugin.getPluginID(), CLogger.Level.Trace);
        amqp_server = plugin.getConfig().getStringParam("amqp_server", "128.163.217.97");
        amqp_port = plugin.getConfig().getStringParam("amqp_port", "5672");
        amqp_login = plugin.getConfig().getStringParam("amqp_login", "tester");
        amqp_password = plugin.getConfig().getStringParam("amqp_password", "tester01");
    }

    @GET
    @Path("badpacket")
    @Produces(MediaType.TEXT_PLAIN + ";charset=utf-8")
    public Response badPacket() {
        logger.trace("Call to badPacket()");
        try {
            MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Issuing command to start badpacket executor");
            enable.setParam("src_region", plugin.getRegion());
            enable.setParam("src_agent", plugin.getAgent());
            enable.setParam("src_plugin", plugin.getPluginID());
            enable.setParam("dst_region", plugin.getRegion());
            enable.setParam("dst_agent", plugin.getAgent());
            enable.setParam("watchdogtimer", "5000");
            enable.setParam("action", "enable");
            enable.setParam("configparams", "pluginname=executor-plugin" +
                    ",jarfile=executor/target/executor-plugin-0.1.0.jar" +
                    ",dstPlugin=" + plugin.getPluginID() +
                    ",runCommand=sendudp e4:1d:2d:0e:a6:c0 128.163.202.51 8080 p2p2 10");
            plugin.sendMsgEvent(enable);
            return Response.ok("Program starting...").header("Access-Control-Allow-Origin", "*").build();
        } catch (Exception e) {
            logger.error("badPacket() : {}", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error : " +
                    e.getMessage()).header("Access-Control-Allow-Origin", "*").build();
        }
    }

    @GET
    @Path("kanon")
    @Produces(MediaType.TEXT_PLAIN + ";charset=utf-8")
    public Response kAnon() {
        logger.trace("Call to kAnon()");
        try {
            MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Issuing command to start kanon executor");
            enable.setParam("src_region", plugin.getRegion());
            enable.setParam("src_agent", plugin.getAgent());
            enable.setParam("src_plugin", plugin.getPluginID());
            enable.setParam("dst_region", plugin.getRegion());
            enable.setParam("dst_agent", plugin.getAgent());
            enable.setParam("watchdogtimer", "5000");
            enable.setParam("action", "enable");
            enable.setParam("configparams", "pluginname=executor-plugin" +
                    ",jarfile=executor/target/executor-plugin-0.1.0.jar" +
                    ",dstPlugin=" + plugin.getPluginID() +
                    ",requiresSudo=false" +
                    ",runCommand=kanon 10000 100 " + amqp_server + " " + amqp_port + " pmacct kanonex_read " + amqp_server + " " + amqp_port + " kanonex_write_e 1 0 0 'irnc_user' 'u$export01' 'irnc_user' 'u$export01'");
            plugin.sendMsgEvent(enable);
            return Response.ok("Program starting...").header("Access-Control-Allow-Origin", "*").build();
        } catch (Exception e) {
            logger.error("kAnon() : {}", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error : " +
                    e.getMessage()).header("Access-Control-Allow-Origin", "*").build();
        }
    }

    public String generateCADL(String runcommand, String targetLocation) {
        //MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
        //me.setParam("globalcmd", Boolean.TRUE.toString());

        //me.setParam("action", "gpipelinesubmit");
        //me.setParam("action_tenantid","0");

        Gson gson = new GsonBuilder().create();

        Map<String,String> n0Params = new HashMap<>();
        n0Params.put("pluginname","executor-plugin");
        n0Params.put("jarfile","executor/target/executor-plugin-0.1.0.jar");
        n0Params.put("runCommand", runcommand);
        n0Params.put("location",targetLocation);
        n0Params.put("watchdogtimer", "5000");
        //add information related to issuing plugin
        n0Params.put("dstRegion", plugin.getRegion());
        n0Params.put("dstAgent", plugin.getAgent());
        n0Params.put("dstPlugin", plugin.getPluginID());


        List<gNode> gNodes = new ArrayList<>();
        gNodes.add(new gNode("dummy", "uk", "0", n0Params));
        gEdge e0 = new gEdge("0","1000000","1000000");

        List<gEdge> gEdges = new ArrayList<>();
        gEdges.add(e0);

        gPayload gpay = new gPayload(gNodes,gEdges);
        gpay.pipeline_id = "0";
        gpay.pipeline_name = "demo_pipeline";
        String json = gson.toJson(gpay);
        return json;

    }

    @GET
    @Path("submit/{args:.*}")
    @Produces(MediaType.TEXT_PLAIN + ";charset=utf-8")
    public Response submit(@PathParam("args") String args) {
        logger.trace("Call to submit({})", args);
        String[] parsedArgs = args.split(" ");
        if (parsedArgs.length < 2) {
            return Response.status(Response.Status.OK).entity("Invalid arguments issued: " + args)
                    .header("Access-Control-Allow-Origin", "*").build();
        }
        String targetLocation = parsedArgs[0];
        String program = parsedArgs[1];

            try {
                SimpleDateFormat getTimeZone = new SimpleDateFormat("zzz");
                String timezone = getTimeZone.format(new Date());
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-zzz");
                //String[] parsedArgs = args.split(" ");
                //String program = "";
                //if (parsedArgs.length >= 1)
                //    program = parsedArgs[0];
                String programArgs = "";
                Date start = new Date();
                Date end = new Date();
                Calendar temp_er = Calendar.getInstance();
                temp_er.add(Calendar.SECOND, 60);
                end = temp_er.getTime();

                if (program.equals("perfSONAR_Throughput")) {
                    if (parsedArgs.length >= 5) {
                        Calendar temp = Calendar.getInstance();
                        temp.add(Calendar.SECOND, 60 + Integer.parseInt(parsedArgs[4]));
                        end = temp.getTime();
                    }
                    for (int i = 2; i < parsedArgs.length; i++) {
                        programArgs += parsedArgs[i] + " ";
                    }
                    programArgs = programArgs.trim();
                } else {
                    if (parsedArgs.length >= 3) {
                        try {
                            start = formatter.parse(parsedArgs[2] + "-" + timezone);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    if (parsedArgs.length >= 4) {
                        try {
                            end = formatter.parse(parsedArgs[3] + "-" + timezone);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    if (parsedArgs.length >= 5) {
                        for (int i = 4; i < parsedArgs.length; i++) {
                            programArgs += parsedArgs[i] + " ";
                        }
                        programArgs = programArgs.trim();
                    }
                }
                //String amqp_server = "128.163.217.97";
                //String amqp_port = "5672";
                //String amqp_login = "tester";
                //String amqp_password = "tester01";

                MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                enable.setParam("src_region", plugin.getRegion());
                enable.setParam("src_agent", plugin.getAgent());
                enable.setParam("src_plugin", plugin.getPluginID());
                enable.setParam("dst_region", plugin.getRegion());
                //enable.setParam("dst_agent", plugin.getAgent());
                //enable.setParam("dst_agent", targetLocation);

                enable.setParam("globalcmd", Boolean.TRUE.toString());
                enable.setParam("action", "gpipelinesubmit");
                enable.setParam("action_tenantid","0");


                String amqp_exchange = java.util.UUID.randomUUID().toString();
                QueueListener listener = new QueueListener(amqp_server, amqp_login, amqp_password, amqp_exchange, program,
                        start, end, programArgs);
                new Thread(listener).start();
                listeners.put(amqp_exchange, listener);

                String runCommand = args.substring(args.indexOf(" ") + 1).replaceAll(",", "\\,") + " " + amqp_server + " " + amqp_port + " " + amqp_login + " " + amqp_password + " " + amqp_exchange;
                enable.setParam("gpipeline_compressed",String.valueOf(Boolean.TRUE));
                enable.setCompressedParam("action_gpipeline",generateCADL(runCommand,targetLocation));


                /*
                enable.setParam("configparams", "pluginname=executor-plugin" +
                        ",jarfile=executor/target/executor-plugin-0.1.0.jar" +
                        ",dstPlugin=" + plugin.getPluginID() +
                        ",runCommand=" + args.substring(args.indexOf(" ") + 1).replaceAll(",", "\\,") + " " + amqp_server + " " + amqp_port + " " +
                        amqp_login + " " + amqp_password + " " + amqp_exchange);
*/

                try {
                    MsgEvent response = plugin.sendRPC(enable);

                    //String  gpipelineString = response.getCompressedParam("action_gpipeline");

                    //Gson gson = new GsonBuilder().create();
                    //gPayload me = gson.fromJson(gpipelineString, gPayload.class);

                    String pipeline_id = response.getParam("gpipeline_id");
                    logger.error("Main  pipeline_id: " + pipeline_id);

                    Thread.sleep(3000);

                    int status = getPipelineStatus(pipeline_id);



                    while(!(status == 10) && (status != -1)) {
                        if(status == -1) {
                            logger.error("Problem with Pipeline Check ! Status -1");
                        }
                        status = getPipelineStatus(pipeline_id);
                        logger.error("pipeline_id: " + pipeline_id + " status:" + status);
                        Thread.sleep(5000);
                    }



/*
                    for(gNode node : me.nodes) {
                        logger.error("node_id: " + node.node_id + " node_name:" + node.node_name + " node: " + node.params);
                   }
                   */
                    //logger.error(response.getParams().toString());


                    if (response != null) {
                        listener.setPluginID(response.getParam("plugin"));
                    }
                    return Response.ok(amqp_exchange).header("Access-Control-Allow-Origin", "*").build();
                } catch (Exception ex) {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                            .header("Access-Control-Allow-Origin", "*").build();
                }
            } catch (Exception e) {
                e.printStackTrace();
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                        .header("Access-Control-Allow-Origin", "*").build();
            }

    }

    private int getPipelineStatus(String pipeline_id) {
        int status = -1;
        try {

            logger.error("getPipelineStatus() pipeline_id: " + pipeline_id);

            MsgEvent pipelineCheck = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Checking Pipeline");
            pipelineCheck.setParam("src_region", plugin.getRegion());
            pipelineCheck.setParam("src_agent", plugin.getAgent());
            pipelineCheck.setParam("src_plugin", plugin.getPluginID());
            pipelineCheck.setParam("dst_region", plugin.getRegion());
            //enable.setParam("dst_agent", plugin.getAgent());
            //enable.setParam("dst_agent", targetLocation);

            pipelineCheck.setParam("globalcmd", Boolean.TRUE.toString());
            pipelineCheck.setParam("action", "getgpipelinestatus");
            pipelineCheck.setParam("action_pipeline",pipeline_id);

            //Gson gson = new GsonBuilder().create();
            //gPayload me = gson.fromJson(gpipelineString, gPayload.class);


            //pipelineinfo
            MsgEvent response = plugin.sendRPC(pipelineCheck);
            String pipelineinfo = response.getCompressedParam("pipelineinfo");
            logger.error("pipelineinfo: " + pipelineinfo);

            //List<pipelineStatus> pStatus = pipelineStatusFromJson(pipelineinfo);

            pipelineStatus pStatus = new Gson().fromJson(pipelineinfo,pipelineStatus.class);

            logger.error("pStatus: ");

            //List<Pipeline> pPipeline = pStatus.getPipelines().get(0).getStatusCode());
            status = Integer.parseInt(pStatus.getPipelines().get(0).getStatusCode());

        } catch(Exception ex) {
            logger.error("getPipelineStatus() Error " + ex.toString());
        }

        return status;
    }

    private List<pipelineStatus> pipelineStatusFromJson(String json) {

        Type listType = new TypeToken<ArrayList<pipelineStatus>>(){}.getType();

        List<pipelineStatus> yourClassList = new Gson().fromJson(json, listType);

        //return gson.fromJson(json, Payload.class);
        return yourClassList;

    }


/*
    @GET
    @Path("submit/{args:.*}")
    @Produces(MediaType.TEXT_PLAIN + ";charset=utf-8")
    public Response submit(@PathParam("args") String args) {
        logger.trace("Call to submit({})", args);
        String[] parsedArgs = args.split(" ");
        if (parsedArgs.length < 2) {
            return Response.status(Response.Status.OK).entity("Invalid arguments issued: " + args)
                    .header("Access-Control-Allow-Origin", "*").build();
        }
        String targetLocation = parsedArgs[0];
        String program = parsedArgs[1];
        if (program.equals("kanon")) {
            try {
                MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                enable.setParam("src_region", plugin.getRegion());
                enable.setParam("src_agent", plugin.getAgent());
                enable.setParam("src_plugin", plugin.getPluginID());
                enable.setParam("dst_region", plugin.getRegion());
                //enable.setParam("dst_agent", plugin.getAgent());
                logger.error("getAgent:" + plugin.getAgent() + " targetLocation:" + targetLocation);
                enable.setParam("dst_agent", targetLocation);
                enable.setParam("watchdogtimer", "5000");
                enable.setParam("action", "add");
                String watcherID = java.util.UUID.randomUUID().toString();
                KanonWatcher watcher = new KanonWatcher(watcherID, args.substring(args.indexOf(" ") + 1));
                new Thread(watcher).start();
                watchers.put(watcherID, watcher);
                enable.setParam("configparams", "pluginname=executor-plugin" +
                        ",jarfile=executor/target/executor-plugin-0.1.0.jar" +
                        ",dstPlugin=" + plugin.getPluginID() +
                        ",requiresSudo=false" +
                        ",runCommand=" + args.substring(args.indexOf(" ") + 1));
                try {
                    logger.error(enable.getParams().toString());
                    MsgEvent response = plugin.sendRPC(enable);
                    //plugin.sendMsgEvent(enable);
                    //MsgEvent response = enable;
                    if (response != null)
                        watcher.setPluginID(response.getParam("plugin"));
                    return Response.ok(watcherID).header("Access-Control-Allow-Origin", "*").build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                            .header("Access-Control-Allow-Origin", "*").build();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                        .header("Access-Control-Allow-Origin", "*").build();
            }
        } else {
            try {
                SimpleDateFormat getTimeZone = new SimpleDateFormat("zzz");
                String timezone = getTimeZone.format(new Date());
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-zzz");
                //String[] parsedArgs = args.split(" ");
                //String program = "";
                //if (parsedArgs.length >= 1)
                //    program = parsedArgs[0];
                String programArgs = "";
                Date start = new Date();
                Date end = new Date();
                if (program.equals("perfSONAR_Throughput")) {
                    if (parsedArgs.length >= 5) {
                        Calendar temp = Calendar.getInstance();
                        temp.add(Calendar.SECOND, 60 + Integer.parseInt(parsedArgs[4]));
                        end = temp.getTime();
                    }
                    for (int i = 2; i < parsedArgs.length; i++) {
                        programArgs += parsedArgs[i] + " ";
                    }
                    programArgs = programArgs.trim();
                } else {
                    if (parsedArgs.length >= 3) {
                        try {
                            start = formatter.parse(parsedArgs[2] + "-" + timezone);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    if (parsedArgs.length >= 4) {
                        try {
                            end = formatter.parse(parsedArgs[3] + "-" + timezone);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    if (parsedArgs.length >= 5) {
                        for (int i = 4; i < parsedArgs.length; i++) {
                            programArgs += parsedArgs[i] + " ";
                        }
                        programArgs = programArgs.trim();
                    }
                }
                //String amqp_server = "128.163.217.97";
                //String amqp_port = "5672";
                //String amqp_login = "tester";
                //String amqp_password = "tester01";
                MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                enable.setParam("src_region", plugin.getRegion());
                enable.setParam("src_agent", plugin.getAgent());
                enable.setParam("src_plugin", plugin.getPluginID());
                enable.setParam("dst_region", plugin.getRegion());
                //enable.setParam("dst_agent", plugin.getAgent());
                enable.setParam("dst_agent", targetLocation);
                enable.setParam("watchdogtimer", "5000");
                enable.setParam("action", "add");
                String amqp_exchange = java.util.UUID.randomUUID().toString();
                QueueListener listener = new QueueListener(amqp_server, amqp_login, amqp_password, amqp_exchange, program,
                        start, end, programArgs);
                new Thread(listener).start();
                listeners.put(amqp_exchange, listener);
                enable.setParam("configparams", "pluginname=executor-plugin" +
                        ",jarfile=executor/target/executor-plugin-0.1.0.jar" +
                        ",dstPlugin=" + plugin.getPluginID() +
                        ",runCommand=" + args.substring(args.indexOf(" ") + 1).replaceAll(",", "\\,") + " " + amqp_server + " " + amqp_port + " " +
                        amqp_login + " " + amqp_password + " " + amqp_exchange);

                try {
                    logger.error(enable.getParams().toString());
                    MsgEvent response = plugin.sendRPC(enable);
                    if (response != null) {
                        listener.setPluginID(response.getParam("plugin"));
                    }
                    return Response.ok(amqp_exchange).header("Access-Control-Allow-Origin", "*").build();
                } catch (Exception ex) {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                            .header("Access-Control-Allow-Origin", "*").build();
                }
            } catch (Exception e) {
                e.printStackTrace();
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                        .header("Access-Control-Allow-Origin", "*").build();
            }
        }
    }
*/

    @GET
    @Path("list")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response list() {
        logger.trace("Call to list()");
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (QueueListener listener : listeners.values()) {
                sb.append(listener.getListing());
                sb.append(",");
            }
            if (listeners.size() > 0) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
            sb.append("]");
            return Response.ok(sb.toString()).header("Access-Control-Allow-Origin", "*").build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(500).entity("An exception has occured!")
                .header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/list/kanon")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response kAnonList() {
        logger.trace("Call to kAnonList()");
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (KanonWatcher watcher : watchers.values()) {
                sb.append(watcher.getListing());
                sb.append(",");
            }
            if (watchers.size() > 0)
                sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append("]");
            return Response.ok(sb.toString()).header("Access-Control-Allow-Origin", "*").build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(500).entity("An exception has occured!")
                .header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("results/{amqp_exchange}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response results(@PathParam("amqp_exchange") String amqp_exchange) {
        logger.trace("Call to results()");
        logger.debug("amqp_exchange: {}", amqp_exchange);
        try {
            QueueListener listener = listeners.get(amqp_exchange);
            if (listener != null) {
                return Response.ok(listener.Results()).header("Access-Control-Allow-Origin", "*").build();
            } else {
                return Response.ok(listeners.keySet().toString()).header("Access-Control-Allow-Origin", "*").build();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(500).entity("No such exchange found!")
                .header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("close/{amqp_exchange}")
    @Produces(MediaType.TEXT_PLAIN + ";charset=utf-8")
    public Response closeListener(@PathParam("amqp_exchange") String amqp_exchange) {
        logger.trace("Call to closeListener()");
        logger.debug("amqp_exchange: {}", amqp_exchange);
        QueueListener listener;
        if ((listener = listeners.get(amqp_exchange)) != null) {
            listener.kill();
            listeners.remove(amqp_exchange);
            return Response.ok("QueryListener Disposed").header("Access-Control-Allow-Origin", "*").build();
        }
        KanonWatcher watcher;
        if ((watcher = watchers.get(amqp_exchange)) != null) {
            watcher.kill();
            watchers.remove(amqp_exchange);
            return Response.ok("KanonWatcher Disposed").header("Access-Control-Allow-Origin", "*").build();
        }
        return Response.status(500).entity("No such listener or watcher found or has already been closed!")
                .header("Access-Control-Allow-Origin", "*").build();
    }

    public static class QueueListener implements Runnable {
        private static final long LISTENER_TIMEOUT = 1000 * 60 * 5;
        private static final long RESULTS_TIMEOUT = 1000 * 60 * 60;
        private String amqp_server;
        private String amqp_login;
        private String amqp_password;
        private String amqp_exchange;
        private String pluginID;
        private String program;
        private Date issued;
        private Date start;
        private Date end;
        private String programArgs;
        private Timer closer;
        private Timer disposer;

        private boolean processing = true;
        private boolean running = true;
        private boolean alive = true;

        private final Object logLock = new Object();
        private HashMap<Long, String> logs = new HashMap<>();
        private final Object resultsLock = new Object();
        private HashSet<String> results = new HashSet<>();

        QueueListener(String amqp_server, String amqp_login, String amqp_password, String amqp_queue_name,
                      String program, Date start, Date end, String programArgs) {
            this.amqp_server = amqp_server;
            this.amqp_login = amqp_login;
            this.amqp_password = amqp_password;
            this.amqp_exchange = amqp_queue_name;
            this.program = program;
            this.issued = new Date();
            this.start = start;
            this.end = end;
            this.programArgs = programArgs;
            this.closer = null;
            this.disposer = null;
        }

        class CloseTask extends TimerTask {
            @Override
            public void run() {
                close();
            }
        }

        class DisposeTask extends TimerTask {
            @Override
            public void run() {
                dispose();
            }
        }

        @Override
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(this.amqp_server);
            factory.setUsername(this.amqp_login);
            factory.setPassword(this.amqp_password);
            factory.setConnectionTimeout(10000);
            Connection connection;
            try {
                try {
                    connection = factory.newConnection();
                } catch (IOException e) {
                    logger.error("Failed to create AMQP connection... {}", e.getMessage());
                    return;
                }
                Channel rx_channel;
                try {
                    rx_channel = connection.createChannel();
                } catch (IOException e) {
                    logger.error("Failed to create receiving channel... {}", e.getMessage());
                    return;
                }
                try {
                    //rx_channel.exchangeDeclare(this.amqp_exchange, "direct");
                    //rx_channel.exchangeDeclare(this.amqp_exchange, "fanout", false, false, true, null);
                    rx_channel.exchangeDeclare(this.amqp_exchange, "fanout");

                    //exchangeDeclare(java.lang.String exchange, java.lang.String type, boolean passive, boolean durable, boolean autoDelete, java.util.Map<java.lang.String,java.lang.Object> arguments)



                } catch (IOException e) {
                    logger.error("Failed to declare exchange... {}", e.getMessage());
                    return;
                }
                String queueName;
                try {
                    queueName = rx_channel.queueDeclare().getQueue();
                } catch (IOException e) {
                    logger.error("Failed to declare queue in exchange... {}", e.getMessage());
                    return;
                }
                try {
                    rx_channel.queueBind(queueName, this.amqp_exchange, "");
                } catch (IOException e) {
                    logger.error("Failed to bind to queue... {}", e.getMessage());
                    return;
                }
                QueueingConsumer consumer;
                try {
                    consumer = new QueueingConsumer(rx_channel);
                    rx_channel.basicConsume(queueName, true, consumer);
                } catch (IOException e) {
                    logger.error("Failed to attach consumer to channel... {}", e.getMessage());
                    return;
                }

                while (this.running) {
                    try {
                        QueueingConsumer.Delivery delivery = consumer.nextDelivery(500);
                        if (delivery != null) {
                            synchronized (this.resultsLock) {
                                this.results.add(new String(delivery.getBody()).trim());
                            }
                        }
                    } catch (InterruptedException e) {
                        logger.error("Queue consumer tapped out early... {}", e.getMessage());
                    }
                }
                try {
                    rx_channel.queueDelete(queueName);
                } catch (IOException e) {
                    logger.error("Failed to close queue... {}", e.getMessage());
                    return;
                }
                try {
                    rx_channel.exchangeDelete(this.amqp_exchange);
                } catch (IOException e) {
                    logger.error("Failed to close exchange... {}", e.getMessage());
                    return;
                }
                try {
                    rx_channel.close();
                } catch (IOException e) {
                    logger.error("Failed to close channel... {}", e.getMessage());
                    return;
                }
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.error("Failed to close connection... {}", e.getMessage());
                }
                try {
                    while (this.alive) {
                        Thread.sleep(1000);
                    }
                    listeners.remove(this.amqp_exchange);
                } catch (InterruptedException e) {
                    // This can happen and is fine
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void log(String ts, String log) {
            synchronized (logLock) {
                this.logs.put(Long.parseLong(ts), log);
            }
        }

        private String getState() {
            if (this.processing)
                return "Executing";
            else if (this.running)
                return "Listening";
            else if (this.alive)
                return "Holding Data";
            else
                return "Shutting Down";
        }

        String getListing() {
            return "{\"exchange\":\"" + this.amqp_exchange +
                    "\",\"state\":\"" + this.getState() +
                    "\",\"issued\":\"" + this.issued +
                    "\",\"start\":\"" + this.start +
                    "\",\"end\":\"" + this.end +
                    "\",\"program\":\"" + this.program +
                    "\",\"args\":\"" + this.programArgs + "\"}";
        }

        String Results() {
            if (this.disposer != null) {
                this.disposer.cancel();
                this.disposer = new Timer();
                this.disposer.schedule(new DisposeTask(), RESULTS_TIMEOUT);
            }
            HashSet<String> resultMessages;
            synchronized (this.resultsLock) {
                resultMessages = new HashSet<>(this.results);
            }
            List<String> logMessages = new ArrayList<>();
            synchronized (this.logLock) {
                List<Long> keys = new ArrayList<>(logs.keySet());
                Collections.sort(keys);
                for (Long key : keys) {
                    logMessages.add(StringEscapeUtils.escapeJavaScript(logs.get(key)));
                }
            }
            StringBuilder ret = new StringBuilder();
            ret.append("{");
            ret.append("\"exchange\":\"");
            ret.append(this.amqp_exchange);
            ret.append("\",\"state\":\"");
            ret.append(this.getState());
            ret.append("\",\"issued\":\"");
            ret.append(this.issued);
            ret.append("\",\"start\":\"");
            ret.append(this.start);
            ret.append("\",\"end\":\"");
            ret.append(this.end);
            ret.append("\",\"program\":\"");
            ret.append(this.program);
            ret.append("\",\"args\":\"");
            ret.append(this.programArgs);
            ret.append("\",\"logs\":[");
            for (String logMessage : logMessages) {
                ret.append("\"");
                ret.append(logMessage);
                ret.append("\",");
            }
            if (logMessages.size() > 0) {
                ret.deleteCharAt(ret.lastIndexOf(","));
            }
            ret.append("],\"results\":[");
            for (String resultMessage : resultMessages) {
                if (resultMessage.startsWith("{")) {
                    ret.append(resultMessage);
                    ret.append(",");
                } else {
                    ret.append("\"");
                    ret.append(resultMessage);
                    ret.append("\",");
                }
            }
            if (resultMessages.size() > 0) {
                ret.deleteCharAt(ret.lastIndexOf(","));
            }
            ret.append("]");
            ret.append("}");
            return ret.toString();
        }

        void setPluginID(String pluginID) {
            this.pluginID = pluginID;
        }

        public void done() {
            this.processing = false;
            this.closer = new Timer();
            this.closer.schedule(new CloseTask(), LISTENER_TIMEOUT);
            this.disposer = new Timer();
            this.disposer.schedule(new DisposeTask(), RESULTS_TIMEOUT);
        }

        public void close() {
            logger.info("Closing Exchange: " + this.amqp_exchange);
            this.running = false;
        }

        void dispose() {
            logger.info("Disposing of Data: " + this.amqp_exchange);
            this.alive = false;
        }

        void kill() {
            logger.info("Destroying QueryListener for: " + this.amqp_exchange);
            if (this.closer != null) {
                this.closer.cancel();
            }
            if (this.disposer != null) {
                this.disposer.cancel();
            }
            if (this.processing) {
                Map<String, String> params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("dst_region", plugin.getRegion());
                params.put("dst_agent", plugin.getAgent());
                params.put("action", "disable");
                params.put("plugin", this.pluginID);
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));
            }
            this.running = false;
            this.alive = false;
        }
    }

    private static class KanonWatcher implements Runnable {
        private String id;
        private String pluginID;
        private String command;
        private Date issued;

        private boolean alive = true;

        private final Object logLock = new Object();
        private HashSet<String> logs = new HashSet<>();

        KanonWatcher(String id, String command) {
            this.id = id;
            this.command = command;
            this.issued = new Date();
        }

        @Override
        public void run() {
            try {
                try {
                    while (alive) {
                        Thread.sleep(1000);
                    }
                    watchers.remove(id);
                } catch (InterruptedException e) {
                    // This can happen and is fine
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void log(String log) {
            synchronized (logLock) {
                logs.add(log);
            }
        }

        String Results() {
            HashSet<String> logMessages;
            synchronized (this.logLock) {
                logMessages = new HashSet<>(this.logs);
            }
            StringBuilder ret = new StringBuilder();
            ret.append("{");
            ret.append("\"state\":\"");
            ret.append(this.getState());
            ret.append("\",\"issued\":\"");
            ret.append(this.issued);
            ret.append("\",\"logs\":[");
            for (String logMessage : logMessages) {
                ret.append("\"");
                ret.append(logMessage);
                ret.append("\",");
            }
            if (logMessages.size() > 0) {
                ret.deleteCharAt(ret.lastIndexOf(","));
            }
            ret.append("]");
            ret.append("}");
            return ret.toString();
        }

        private String getState() {
            if (alive)
                return "Running";
            else
                return "Shutting Down";
        }

        String getListing() {
            return "{\"id\":\"" + id +
                    "\",\"state\":\"" + getState() +
                    "\",\"issued\":\"" + issued +
                    "\",\"command\":\"" + command + "\"}";
        }

        void setPluginID(String pluginID) {
            this.pluginID = pluginID;
        }

        void dispose() {
            this.alive = false;
        }

        void kill() {
            logger.info("Destroying KanonWatcher [{}]", id);
            if (alive) {
                Map<String, String> params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("dst_region", plugin.getRegion());
                params.put("dst_agent", plugin.getAgent());
                params.put("action", "disable");
                params.put("plugin", this.pluginID);
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));
            }
            alive = false;
        }
    }
}
