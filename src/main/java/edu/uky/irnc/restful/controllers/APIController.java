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

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unused")
@Path("API")
public class APIController {

    public static ConcurrentHashMap<String, QueueListener> listeners = new ConcurrentHashMap<>();
    private static Plugin plugin;
    private static CLogger logger;
    private static String amqp_server;
    private static String amqp_port;
    private static String amqp_login;
    private static String amqp_password;

    public static void setPlugin(Plugin mainPlugin) {
        plugin = mainPlugin;
        logger = new CLogger(APIController.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                plugin.getPluginID(), CLogger.Level.Info);
        amqp_server = plugin.getConfig().getStringParam("amqp_server", "128.163.217.97");
        amqp_port = plugin.getConfig().getStringParam("amqp_port", "5672");
        amqp_login = plugin.getConfig().getStringParam("amqp_login", "tester");
        amqp_password = plugin.getConfig().getStringParam("amqp_password", "tester01");
    }

    @POST
    @Path("/add")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response crunchifyREST11(InputStream incomingData) {

        StringBuilder crunchifyBuilder = new StringBuilder();
        String returnString = null;
        try {

            BufferedReader in = new BufferedReader(new InputStreamReader(incomingData));
            String line = null;
            while ((line = in.readLine()) != null) {
                crunchifyBuilder.append(line);
            }

            String jsonString = crunchifyBuilder.toString();

            mApp app = new Gson().fromJson(jsonString, mApp.class);

            String qhost = amqp_server;
            String qport = amqp_port;
            String qlogin = amqp_login;
            String qpassword = amqp_password;
            String qname = UUID.randomUUID().toString();

            //determine if queue was provided
            for (mNode node : app.nodes) {
                if((node.qhost != null) && (node.qport != null) && (node.qlogin != null) && (node.qpassword != null) && (node.qname != null)) {
                    qhost = node.qhost;
                    qport = node.qport;
                    qlogin = node.qlogin;
                    qpassword = node.qpassword;
                    qname = node.qname;
                }
            }

            for (mNode node : app.nodes) {
                node.qhost = qhost;
                node.qport = qport;
                node.qlogin = qlogin;
                node.qpassword = qpassword;
                node.qname = qname;
            }

            //get queue ready
            Date start = new Date();
            Date end = new Date();
            Calendar temp_er = Calendar.getInstance();
            temp_er.add(Calendar.SECOND, app.duration * 3);
            end = temp_er.getTime();


            QueueListener listener = new QueueListener(qhost, qlogin, qpassword, qname, app.name, jsonString, app);
            new Thread(listener).start();
            listeners.put(qname, listener);

            String cadlJSON = mAppToCADL(app);

            String pipeline_id = addApplication(cadlJSON);

            try {

                if (pipeline_id != null) {

                    listeners.get(qname).getApp().id = pipeline_id;

                    int status = -2;

                    while(!(status == 10) && (status != -1)) {
                        Thread.sleep(1000);
                        if(status == -1) {
                            logger.error("Problem with Pipeline Check ! Status -1");
                        }
                        status = getPipelineStatus(pipeline_id);
                        logger.info("pipeline_id: " + pipeline_id + " status:" + status);
                    }

                } else {
                    logger.error("pipeline_id = null");
                }


                returnString = qname;


                //return Response.ok(qname).header("Access-Control-Allow-Origin", "*").build();
            } catch (Exception ex) {

                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                String exceptionAsString = sw.toString();
                logger.error(exceptionAsString);

                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                        .header("Access-Control-Allow-Origin", "*").build();
            }

        } catch (Exception e) {
            logger.error("Error Parsing: - ");
        }
        logger.debug("Data Received: " + crunchifyBuilder.toString());

        // return HTTP response 200 in case of success
        //return Response.status(200).entity("woot2").build();
        //return Response.ok(returnString, MediaType.APPLICATION_JSON_TYPE).build();
        return Response.ok(returnString).header("Access-Control-Allow-Origin", "*").build();
    }

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
    @Path("status/{amqp_exchange}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response status(@PathParam("amqp_exchange") String amqp_exchange) {
        logger.trace("Call to start()");
        logger.debug("amqp_exchange: {}", amqp_exchange);
        try {

            String pipeline_id = listeners.get(amqp_exchange).getApp().id;
            //applicaiton is readed enable it
            String status_code = "-1";
            if(statusApplication(pipeline_id)) {
                status_code = "10";
            } else {
                status_code = "9";
            }
            return Response.ok(status_code).header("Access-Control-Allow-Origin", "*").build();


        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(500).entity("No such exchange found!")
                .header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("start/{amqp_exchange}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response start(@PathParam("amqp_exchange") String amqp_exchange) {
        logger.trace("Call to start()");
        logger.debug("amqp_exchange: {}", amqp_exchange);
        try {

            QueueListener listener = listeners.get(amqp_exchange);
            if (listener != null) {
                //clear results on start in case of restart
                if(!listener.results.isEmpty()) {
                    listener.clearResults();
                }
            }

            String pipeline_id = listeners.get(amqp_exchange).getApp().id;

            //applicaiton is readed enable it
            String status_code = "-1";
            if(enableApplication(pipeline_id)) {
                status_code = "10";
            } else {
                status_code = "9";
            }

            return Response.ok(status_code).header("Access-Control-Allow-Origin", "*").build();


        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(500).entity("No such exchange found!")
                .header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("stop/{amqp_exchange}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response stop(@PathParam("amqp_exchange") String amqp_exchange) {
        logger.trace("Call to start()");
        logger.debug("amqp_exchange: {}", amqp_exchange);
        try {

            ////end_process
            String pipeline_id = listeners.get(amqp_exchange).getApp().id;
            //applicaiton is readed enable it
            String status_code = "-1";
            if(disableApplication(pipeline_id)) {
                status_code = "10";
            } else {
                status_code = "9";
            }

            return Response.ok(status_code).header("Access-Control-Allow-Origin", "*").build();


        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(500).entity("No such exchange found!")
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
        try {
            logger.trace("Call to closeListener()");
            logger.debug("amqp_exchange: {}", amqp_exchange);
            String pipeline_id = listeners.get(amqp_exchange).getApp().id;

            QueueListener listener;
            if ((listener = listeners.get(amqp_exchange)) != null) {
                listener.kill();
                listeners.remove(amqp_exchange);
                //todo remove plugins
                //remove plugins
                if (!removeApplication(pipeline_id)) {
                    logger.error("Could not remove application: " + pipeline_id + " exchange_id:" + amqp_exchange);
                }
                return Response.ok("QueryListener Disposed").header("Access-Control-Allow-Origin", "*").build();
            }
        } catch(Exception ex) {

            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(" CLOSE EXCHANGE ERROR " + errors.toString());

            return Response.status(500).entity("Error on Close!")
                    .header("Access-Control-Allow-Origin", "*").build();
        }
        return Response.status(500).entity("No such listener or watcher found or has already been closed!")
                .header("Access-Control-Allow-Origin", "*").build();
    }

    public static class QueueListener implements Runnable {
        private static final long LISTENER_TIMEOUT = 1000 * 60 * 5;
        private static final long RESULTS_TIMEOUT = 1000 * 60 * 60;
        private mApp app;
        private String amqp_server;
        private String amqp_login;
        private String amqp_password;
        private String amqp_exchange;
        private String pluginID;
        private String program;
        private Date issued;
        private Timer closer;
        private Timer disposer;
        private String pipeline_id;

        private boolean processing = true;
        private boolean running = true;
        private boolean alive = true;

        private final Object logLock = new Object();
        private HashMap<Long, String> logs = new HashMap<>();
        private final Object resultsLock = new Object();
        //private HashSet<String> results = new HashSet<>();
        private String results = new String();

        QueueListener(String amqp_server, String amqp_login, String amqp_password, String amqp_queue_name,
                      String program, String jsonString, mApp app) {
            this.app = app;
            this.amqp_server = amqp_server;
            this.amqp_login = amqp_login;
            this.amqp_password = amqp_password;
            this.amqp_exchange = amqp_queue_name;
            this.program = program;
            this.issued = new Date();
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

        public void clearResults() {
            this.results = null;
        }

        /*
        public void setPipelineId(String pipeline_id) {
            this.pipeline_id = pipeline_id;
        }

        public String getPipelineId() {
            return pipeline_id;
        }
        */

        public mApp getApp() {
            return this.app;
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

                                results = new String(delivery.getBody()).trim();
                                //String str = new String(delivery.getBody()).trim();
                                //logger.error("*" + str + "*");
                                //this.results.add(str);
                                //this.results.add(new String(delivery.getBody()).trim());
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
                    "\",\"program\":\"" + this.program +
                    "\"}";
        }

        String Results() {
            if (this.disposer != null) {
                this.disposer.cancel();
                this.disposer = new Timer();
                this.disposer.schedule(new DisposeTask(), RESULTS_TIMEOUT);
            }
            /*
            HashSet<String> resultMessages;
            synchronized (this.resultsLock) {
                resultMessages = new HashSet<>(this.results);
            }
            */

            String resultMessage = null;
            synchronized (this.resultsLock) {
                resultMessage = this.results;
                if(resultMessage.length() == 0) {
                    resultMessage = "[]";
                }
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
            ret.append("\",\"program\":\"");
            ret.append(this.program);
            ret.append("\",\"logs\":[");
            for (String logMessage : logMessages) {
                ret.append("\"");
                ret.append(logMessage);
                ret.append("\",");
            }
            if (logMessages.size() > 0) {
                ret.deleteCharAt(ret.lastIndexOf(","));
            }

            ret.append("],\"results\":" + resultMessage);

            /*
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
            */
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

    public String mAppToCADL(mApp app) {
        String cadlJSON = null;

        try {
            Gson gson = new GsonBuilder().create();

            List<gNode> gNodes = new ArrayList<>();
            List<gEdge> gEdges = new ArrayList<>();

            for (mNode node : app.nodes) {
                logger.info("Node name: " + node.name + " type:" + node.type + " command:" + node.commands);

                String queueAppend = node.qhost + " " + node.qport + " " + node.qlogin + " " + node.qpassword + " " + node.qname;
                String startingDir = "/home/acanets/AMIS/amis/uml/";
                String type = node.type;
                if(type.startsWith("/")) {
                    type = type.replaceFirst("/","");
                }
                String runcommand = startingDir + type + " " + app.duration + " " + node.commands + " " + queueAppend;

                Map<String, String> n0Params = new HashMap<>();
                n0Params.put("pluginname", "executor-plugin");
                n0Params.put("jarfile", "executor-plugin-0.1.0.jar");

                n0Params.put("runCommand", runcommand);
                n0Params.put("location", node.location);
                n0Params.put("watchdogtimer", "5000");
                //add information related to issuing plugin
                n0Params.put("dstRegion", plugin.getRegion());
                n0Params.put("dstAgent", plugin.getAgent());
                n0Params.put("dstPlugin", plugin.getPluginID());
                gNodes.add(new gNode(node.type, node.name, node.id, n0Params));
            }

            gEdge e0 = new gEdge("0", "1000000", "1000000");
            gEdges.add(e0);


            gPayload gpay = new gPayload(gNodes, gEdges);
            gpay.pipeline_id = app.id;
            gpay.pipeline_name = app.name;
            cadlJSON = gson.toJson(gpay);

        } catch(Exception ex) {
            logger.error("mAppToCADL Error : " + ex.getMessage());
        }

        return cadlJSON;

    }

    public String generateCADL(String runcommand, String targetLocation) {
        //MsgEvent me = new MsgEvent(MsgEvent.Type.CONFIG, null, null, null, "get resourceinventory inventory");
        //me.setParam("globalcmd", Boolean.TRUE.toString());

        //me.setParam("action", "gpipelinesubmit");
        //me.setParam("action_tenantid","0");

        Gson gson = new GsonBuilder().create();

        Map<String,String> n0Params = new HashMap<>();
        n0Params.put("pluginname","executor-plugin");
        n0Params.put("jarfile","executor-plugin-0.1.0.jar");
        n0Params.put("runCommand", runcommand);
        n0Params.put("location",targetLocation);
        n0Params.put("watchdogtimer", "5000");
        //add information related to issuing plugin
        n0Params.put("dstRegion", plugin.getRegion());
        n0Params.put("dstAgent", plugin.getAgent());
        n0Params.put("dstPlugin", plugin.getPluginID());

//APP CONFIG
        /*
        try {
            String command = "60 10  4  11  12  20  21  23  16";
            mApp app = new mApp("my test");
            //mNode(String type, String name, String commands)
            mNode node = new mNode("Netflow-DPDK-NORSSuml/Netflow-DPDK-NORSS/run_exe.sh", "UK Netflow", command);
            node.qname = "exchange-name-0";
            node.qhost = "128.163.217.97";
            node.qport = "5821";
            node.qlogin = "sr";
            node.qpassword = "u$sr01";
            app.nodes.add(node);

            logger.info(gson.toJson(app));
        } catch(Exception ex) {
            logger.error("CADL create error: " + ex.getMessage());
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            logger.error(exceptionAsString);
        }
*/
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

    private boolean enableApplication(String pipeline_id) {
        boolean isEnabled = false;
        try {

            gPayload gpay = getGpayload(pipeline_id);

            for(gNode node : gpay.nodes) {

                String iNodeId = node.params.get("inode_id");
                String ResourceId = node.params.get("resource_id");

                MsgEvent getAgent = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                getAgent.setParam("src_region", plugin.getRegion());
                getAgent.setParam("src_agent", plugin.getAgent());
                getAgent.setParam("src_plugin", plugin.getPluginID());
                getAgent.setParam("dst_region", plugin.getRegion());
                getAgent.setParam("dst_agent", plugin.getAgent());
                getAgent.setParam("dst_plugin", "plugin/0");

                //getAgent.setParam("globalcmd", Boolean.TRUE.toString());
                getAgent.setParam("is_global", Boolean.TRUE.toString());
                getAgent.setParam("is_regional", Boolean.TRUE.toString());


                getAgent.setParam("action", "getisassignmentinfo");
                getAgent.setParam("action_inodeid", iNodeId);
                getAgent.setParam("action_resourceid", ResourceId);

                MsgEvent getAgentResponse = plugin.sendRPC(getAgent);

                String isassignmentinfo = getAgentResponse.getCompressedParam("isassignmentinfo");

                logger.info("ASSIGNMENT INFO : " + isassignmentinfo);

                Gson gson = new Gson();
                Type stringStringMap = new TypeToken<Map<String, String>>(){}.getType();
                Map<String,String> map = gson.fromJson(isassignmentinfo, stringStringMap);

                String region = map.get("region");
                String agent = map.get("agent");
                String pluginId = map.get("plugin");

                logger.info("Starting region: " + region + " agent: " + agent + " plugin: " + pluginId);

                MsgEvent runProcess = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                runProcess.setParam("src_region", plugin.getRegion());
                runProcess.setParam("src_agent", plugin.getAgent());
                runProcess.setParam("src_plugin", plugin.getPluginID());
                runProcess.setParam("dst_region", region);
                runProcess.setParam("dst_agent", agent);
                runProcess.setParam("dst_plugin", pluginId);
                runProcess.setParam("cmd", "run_process");

                logger.info("SENDING MESSAGE! " + runProcess.getParams().toString());
                MsgEvent runProcessResponse = plugin.sendRPC(runProcess);
                logger.info("RETURNED MESSAGE! " + runProcessResponse.getParams().toString());

                if(runProcessResponse.getParam("status") != null) {
                    if(Boolean.parseBoolean(runProcessResponse.getParam("status"))) {
                        logger.info("CODY RUNRESPONSE:  " + node.node_id + " " + runProcessResponse.getParams());
                        isEnabled = true;
                    }
                }


            }

        } catch(Exception ex) {
            logger.error("enableApplication Error " + ex.getMessage());
        }
        return isEnabled;
    }

    private boolean statusApplication(String pipeline_id) {
        boolean isEnabled = false;
        try {

            gPayload gpay = getGpayload(pipeline_id);

            for(gNode node : gpay.nodes) {

                String iNodeId = node.params.get("inode_id");
                String ResourceId = node.params.get("resource_id");

                MsgEvent getAgent = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                getAgent.setParam("src_region", plugin.getRegion());
                getAgent.setParam("src_agent", plugin.getAgent());
                getAgent.setParam("src_plugin", plugin.getPluginID());
                getAgent.setParam("dst_region", plugin.getRegion());
                getAgent.setParam("dst_agent", plugin.getAgent());
                getAgent.setParam("dst_plugin", "plugin/0");

                //getAgent.setParam("globalcmd", Boolean.TRUE.toString());
                getAgent.setParam("is_global", Boolean.TRUE.toString());
                getAgent.setParam("is_regional", Boolean.TRUE.toString());


                getAgent.setParam("action", "getisassignmentinfo");
                getAgent.setParam("action_inodeid", iNodeId);
                getAgent.setParam("action_resourceid", ResourceId);

                MsgEvent getAgentResponse = plugin.sendRPC(getAgent);

                String isassignmentinfo = getAgentResponse.getCompressedParam("isassignmentinfo");

                Gson gson = new Gson();
                Type stringStringMap = new TypeToken<Map<String, String>>(){}.getType();
                Map<String,String> map = gson.fromJson(isassignmentinfo, stringStringMap);

                String region = map.get("region");
                String agent = map.get("agent");
                String pluginId = map.get("plugin");

                MsgEvent runProcess = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                runProcess.setParam("src_region", plugin.getRegion());
                runProcess.setParam("src_agent", plugin.getAgent());
                runProcess.setParam("src_plugin", plugin.getPluginID());
                runProcess.setParam("dst_region", region);
                runProcess.setParam("dst_agent", agent);
                runProcess.setParam("dst_plugin", pluginId);
                runProcess.setParam("cmd", "status_process");

                MsgEvent runProcessResponse = plugin.sendRPC(runProcess);

                if(runProcessResponse.getParam("status") != null) {
                    if(Boolean.parseBoolean(runProcessResponse.getParam("status"))) {
                        logger.info("CODY RUNRESPONSE:  " + node.node_id + " " + runProcessResponse.getParams());
                        isEnabled = true;
                    }
                }

            }

        } catch(Exception ex) {
            logger.error("enableApplication Error " + ex.getMessage());
        }
        return isEnabled;
    }

    private boolean disableApplication(String pipeline_id) {
        boolean isDisabled = false;
        try {

            gPayload gpay = getGpayload(pipeline_id);

            for(gNode node : gpay.nodes) {

                String iNodeId = node.params.get("inode_id");
                String ResourceId = node.params.get("resource_id");

                MsgEvent getAgent = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                getAgent.setParam("src_region", plugin.getRegion());
                getAgent.setParam("src_agent", plugin.getAgent());
                getAgent.setParam("src_plugin", plugin.getPluginID());
                getAgent.setParam("dst_region", plugin.getRegion());
                getAgent.setParam("dst_agent", plugin.getAgent());
                getAgent.setParam("dst_plugin", "plugin/0");


                //getAgent.setParam("globalcmd", Boolean.TRUE.toString());
                getAgent.setParam("is_global", Boolean.TRUE.toString());
                getAgent.setParam("is_regional", Boolean.TRUE.toString());


                getAgent.setParam("action", "getisassignmentinfo");
                getAgent.setParam("action_inodeid", iNodeId);
                getAgent.setParam("action_resourceid", ResourceId);

                MsgEvent getAgentResponse = plugin.sendRPC(getAgent);

                String isassignmentinfo = getAgentResponse.getCompressedParam("isassignmentinfo");

                Gson gson = new Gson();
                Type stringStringMap = new TypeToken<Map<String, String>>(){}.getType();
                Map<String,String> map = gson.fromJson(isassignmentinfo, stringStringMap);

                String region = map.get("region");
                String agent = map.get("agent");
                String pluginId = map.get("plugin");

                MsgEvent runProcess = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), "Issuing command to start program");
                runProcess.setParam("src_region", plugin.getRegion());
                runProcess.setParam("src_agent", plugin.getAgent());
                runProcess.setParam("src_plugin", plugin.getPluginID());
                runProcess.setParam("dst_region", region);
                runProcess.setParam("dst_agent", agent);
                runProcess.setParam("dst_plugin", pluginId);
                runProcess.setParam("cmd", "end_process");

                MsgEvent runProcessResponse = plugin.sendRPC(runProcess);

                if(runProcessResponse.getParam("status") != null) {
                    if(Boolean.parseBoolean(runProcessResponse.getParam("status"))) {
                        logger.info("CODY RUNRESPONSE:  " + node.node_id + " " + runProcessResponse.getParams());
                        isDisabled = true;
                    }
                }

            }

        } catch(Exception ex) {
            logger.error("enableApplication Error " + ex.getMessage());
        }
        return isDisabled;
    }

    private gPayload getGpayload(String pipeline_id) {
        gPayload gpay = null;
        try {

            MsgEvent getpipeline = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Issuing command to start program");
            getpipeline.setParam("src_region", plugin.getRegion());
            getpipeline.setParam("src_agent", plugin.getAgent());
            getpipeline.setParam("src_plugin", plugin.getPluginID());
            getpipeline.setParam("dst_region", plugin.getRegion());
            getpipeline.setParam("dst_agent", plugin.getAgent());
            getpipeline.setParam("dst_plugin", "plugin/0");


            //getpipeline.setParam("globalcmd", Boolean.TRUE.toString());
            getpipeline.setParam("is_global", Boolean.TRUE.toString());
            getpipeline.setParam("is_regional", Boolean.TRUE.toString());


            getpipeline.setParam("action", "getgpipeline");
            getpipeline.setParam("action_pipelineid",pipeline_id);

            MsgEvent getPipelineResponse = plugin.sendRPC(getpipeline);

            //logger.error("CODY: " + getPipelineResponse.getParams());
            String pipelineJSON = getPipelineResponse.getCompressedParam("gpipeline");

            gpay = gPayLoadFromJson(pipelineJSON);


        } catch(Exception ex) {
            logger.error("getGpayload Error " + ex.getMessage());
        }
        return gpay;
    }

    public Map<String,String> getMapFromString(String param, boolean isRestricted) {
        Map<String,String> paramMap = null;


        try{
            String[] sparam = param.split(",");

            paramMap = new HashMap<String,String>();

            for(String str : sparam)
            {
                String[] sstr = str.split(":");

                if(isRestricted)
                {
                    paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                }
                else
                {
                    if(sstr.length > 1)
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), URLDecoder.decode(sstr[1], "UTF-8"));
                    }
                    else
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                    }
                }
            }
        }
        catch(Exception ex)
        {
            System.out.println("getMapFromString Error: " + ex.toString());
        }

        return paramMap;
    }

    public String JsonFromgPayLoad(gPayload gpay) {
        Gson gson = new GsonBuilder().create();
        //gPayload me = gson.fromJson(json, gPayload.class);
        //logger.debug(p);
        return gson.toJson(gpay);

    }

    public gPayload gPayLoadFromJson(String json) {
        Gson gson = new GsonBuilder().create();
        gPayload me = gson.fromJson(json, gPayload.class);
        //logger.debug(p);
        return me;
    }

    private int getPipelineStatus(String pipeline_id) {
        int status = -1;
        try {

            //logger.error("getPipelineStatus() pipeline_id: " + pipeline_id);

            MsgEvent pipelineCheck = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Checking Pipeline");
            pipelineCheck.setParam("src_region", plugin.getRegion());
            pipelineCheck.setParam("src_agent", plugin.getAgent());
            pipelineCheck.setParam("src_plugin", plugin.getPluginID());
            pipelineCheck.setParam("dst_region", plugin.getRegion());
            pipelineCheck.setParam("dst_agent", plugin.getAgent());
            pipelineCheck.setParam("dst_plugin", "plugin/0");

            //enable.setParam("dst_agent", plugin.getAgent());
            //enable.setParam("dst_agent", targetLocation);

            //pipelineCheck.setParam("globalcmd", Boolean.TRUE.toString());\
            pipelineCheck.setParam("is_global", Boolean.TRUE.toString());
            pipelineCheck.setParam("is_regional", Boolean.TRUE.toString());


            pipelineCheck.setParam("action", "getgpipelinestatus");
            pipelineCheck.setParam("action_pipeline",pipeline_id);

            //Gson gson = new GsonBuilder().create();
            //gPayload me = gson.fromJson(gpipelineString, gPayload.class);


            //pipelineinfo
            MsgEvent response = plugin.sendRPC(pipelineCheck);
            String pipelineinfo = response.getCompressedParam("pipelineinfo");
            logger.debug("pipelineinfo: " + pipelineinfo);

            //List<pipelineStatus> pStatus = pipelineStatusFromJson(pipelineinfo);

            pipelineStatus pStatus = new Gson().fromJson(pipelineinfo,pipelineStatus.class);

            logger.debug("pStatus: ");

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

    private boolean removeApplication(String pipeline_id) {
        boolean isRemoved = false;
        try {

            MsgEvent remove = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Issuing command to start program");
            remove.setParam("src_region", plugin.getRegion());
            remove.setParam("src_agent", plugin.getAgent());
            remove.setParam("src_plugin", plugin.getPluginID());
            //remove.setParam("dst_region", plugin.getRegion());
            remove.setParam("dst_region", plugin.getRegion());
            remove.setParam("dst_agent", plugin.getAgent());
            remove.setParam("dst_plugin", "plugin/0");

            //remove.setParam("globalcmd", Boolean.TRUE.toString());
            remove.setParam("is_global", Boolean.TRUE.toString());
            remove.setParam("is_regional", Boolean.TRUE.toString());



            remove.setParam("action", "gpipelineremove");
            remove.setParam("action_pipelineid", pipeline_id);

            MsgEvent response = plugin.sendRPC(remove);

            if(response.getParam("success") != null) {

                if(Boolean.parseBoolean(response.getParam("success"))) {
                    isRemoved = true;
                }

            }

        } catch(Exception ex) {
            logger.error("removeApplication Error :" + ex.getMessage());
        }
        return isRemoved;
    }

    private String addApplication(String gPipelineJSON) {
        String pipeline_id = null;
        try {

            MsgEvent add = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Issuing command to start program");
            add.setParam("src_region", plugin.getRegion());
            add.setParam("src_agent", plugin.getAgent());
            add.setParam("src_plugin", plugin.getPluginID());
            add.setParam("dst_region", plugin.getRegion());
            add.setParam("dst_agent", plugin.getAgent());
            add.setParam("dst_plugin", "plugin/0");


            //add.setParam("globalcmd", Boolean.TRUE.toString());
            add.setParam("is_global", Boolean.TRUE.toString());
            add.setParam("is_regional", Boolean.TRUE.toString());

            add.setParam("action", "gpipelinesubmit");
            add.setParam("action_tenantid","0");
            add.setParam("gpipeline_compressed",String.valueOf(Boolean.TRUE));
            add.setCompressedParam("action_gpipeline",gPipelineJSON);

            MsgEvent response = plugin.sendRPC(add);

            logger.info("RESPONSE ADD : " + response.getParams());

            if(response.getParam("gpipeline_id") != null) {

                pipeline_id = response.getParam("gpipeline_id");
            }

        } catch(Exception ex) {
            logger.error("addApplication Error :" + ex.getMessage());
        }
        return pipeline_id;
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


                String amqp_exchange = java.util.UUID.randomUUID().toString();
                QueueListener listener = new QueueListener(amqp_server, amqp_login, amqp_password, amqp_exchange, program,
                        start, end, programArgs);
                new Thread(listener).start();
                listeners.put(amqp_exchange, listener);


                String runCommand = args.substring(args.indexOf(" ") + 1).replaceAll(",", "\\,") + " " + amqp_server + " " + amqp_port + " " + amqp_login + " " + amqp_password + " " + amqp_exchange;
                //enable.setParam("gpipeline_compressed",String.valueOf(Boolean.TRUE));
                //enable.setCompressedParam("action_gpipeline",generateCADL(runCommand,targetLocation));

                String gPipelineJSON = generateCADL(runCommand,targetLocation);
                String pipeline_id = addApplication(gPipelineJSON);


                try {

                    if (pipeline_id != null) {


                        //todo make this much cleaner
                        //adding application and exchange reference
                        activeApplications.put(amqp_exchange,pipeline_id);
                        //Thread.sleep(3000);

                        //int status = getPipelineStatus(pipeline_id);
                        int status = -2;

                        while(!(status == 10) && (status != -1)) {
                            Thread.sleep(3000);
                            if(status == -1) {
                                logger.error("Problem with Pipeline Check ! Status -1");
                            }
                            status = getPipelineStatus(pipeline_id);
                            logger.info("pipeline_id: " + pipeline_id + " status:" + status);
                        }

                        //applicaiton is readed enable it
                        enableApplication(pipeline_id);

                    } else {
                        logger.error("pipeline_id = null");
                    }



                    return Response.ok(amqp_exchange).header("Access-Control-Allow-Origin", "*").build();
                } catch (Exception ex) {

                    StringWriter sw = new StringWriter();
                    ex.printStackTrace(new PrintWriter(sw));
                    String exceptionAsString = sw.toString();
                    logger.error(exceptionAsString);

                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                            .header("Access-Control-Allow-Origin", "*").build();
                }
            } catch (Exception e) {

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionAsString = sw.toString();
                logger.error(exceptionAsString);

                e.printStackTrace();
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error")
                        .header("Access-Control-Allow-Origin", "*").build();
            }

    }

     */
}
