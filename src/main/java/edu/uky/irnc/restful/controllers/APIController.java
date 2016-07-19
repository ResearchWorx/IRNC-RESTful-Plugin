package edu.uky.irnc.restful.controllers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import edu.uky.irnc.restful.Plugin;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
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
    private static boolean badPacketSending = false;

    public static void setPlugin(Plugin mainPlugin) {
        plugin = mainPlugin;
        logger = new CLogger(APIController.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                plugin.getPluginID(), CLogger.Level.Trace);
    }

    @GET
    @Path("badpacket")
    @Produces(MediaType.TEXT_PLAIN + ";charset=utf-8")
    public Response badPacket() {
        if (badPacketSending)
            return Response.ok("Bad Packet being sent...").header("Access-Control-Allow-Origin", "*").build();
        badPacketSending = true;
        logger.trace("Call to badPacket()");
        try {
            MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Issuing command to start badpacket executor");
            enable.setParam("src_region", plugin.getRegion());
            enable.setParam("src_agent", plugin.getAgent());
            enable.setParam("src_plugin", plugin.getPluginID());
            enable.setParam("dst_region", plugin.getRegion());
            enable.setParam("dst_agent", plugin.getAgent());
            enable.setParam("configtype", "pluginadd");
            enable.setParam("configparams", "pluginname=executor-plugin,jarfile=executor/target/executor-plugin-0.1.0.jar" +
                    ",dstPlugin=" + plugin.getPluginID() +
                    ",runCommand=sendudp e4:1d:2d:0e:a6:c0 128.163.202.51 8080 p2p2 10");
            MsgEvent ret = plugin.sendRPC(enable);
            badPacketSending = false;
            return Response.ok("Program starting...").header("Access-Control-Allow-Origin", "*").build();
        } catch (Exception e) {
            logger.error("sendPacket() : {}", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Internal Server Error : " +
                    e.getMessage()).header("Access-Control-Allow-Origin", "*").build();
        }
    }

    @GET
    @Path("submit/{args:.*}")
    @Produces(MediaType.TEXT_PLAIN + ";charset=utf-8")
    public Response getSubmit(@PathParam("args") String args) {
        logger.trace("SUBMIT CALL: \"" + args + "\"");
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-zzz");
            String[] parsedArgs = args.split(" ");
            String program = "";
            if (parsedArgs.length >= 0)
                program = parsedArgs[0];
            Date start = new Date();
            if (parsedArgs.length >= 1) {
                try {
                    start = formatter.parse(parsedArgs[1] + "-EDT");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            Date end = new Date();
            if (parsedArgs.length >= 2) {
                try {
                    end = formatter.parse(parsedArgs[2] + "-EDT");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            String programArgs = "";
            if (parsedArgs.length >= 3) {
                for (int i = 3; i < parsedArgs.length; i++) {
                    programArgs += parsedArgs[i] + " ";
                }
                programArgs = programArgs.trim();
            }
            String amqp_server = "128.163.217.97";
            String amqp_port = "5672";
            String amqp_login = "tester";
            String amqp_password = "tester01";
            MsgEvent enable = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), "Issuing command to start program");
            enable.setParam("src_region", plugin.getRegion());
            enable.setParam("src_agent", plugin.getAgent());
            enable.setParam("src_plugin", plugin.getPluginID());
            enable.setParam("dst_region", plugin.getRegion());
            enable.setParam("dst_agent", plugin.getAgent());
            enable.setParam("configtype", "pluginadd");
            String amqp_exchange = java.util.UUID.randomUUID().toString();
            QueueListener listener = new QueueListener(amqp_server, amqp_login, amqp_password, amqp_exchange, program,
                    start, end, programArgs);
            new Thread(listener).start();
            listeners.put(amqp_exchange, listener);
            enable.setParam("configparams", "pluginname=executor-plugin,jarfile=executor/target/executor-plugin-0.1.0.jar" +
                    ",dstPlugin=" + plugin.getPluginID() +
                    ",runCommand=" + args.replaceAll(",", "\\,") + " " + amqp_server + " " + amqp_port + " " +
                    amqp_login + " " + amqp_password + " " + amqp_exchange);

            try {
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

    @GET
    @Path("list")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response getList() {
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
    @Path("results/{amqp_exchange}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response getResults(@PathParam("amqp_exchange") String amqp_exchange) {
        try {
            QueueListener listener = listeners.get(amqp_exchange);
            if (listener != null) {
                return Response.ok(listener.Results()).header("Access-Control-Allow-Origin", "*").build();
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
        QueueListener listener;
        if ((listener = listeners.get(amqp_exchange)) != null) {
            listener.kill();
            listeners.remove(amqp_exchange);
            return Response.ok("QueryListener Disposed").header("Access-Control-Allow-Origin", "*").build();
        }
        return Response.status(500).entity("No such exchange found or has already been closed!")
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
        private HashSet<String> logs = new HashSet<>();
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
                    rx_channel.exchangeDeclare(this.amqp_exchange, "direct");
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

        public void log(String log) {
            synchronized (logLock) {
                this.logs.add(log);
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
            HashSet<String> logMessages;
            synchronized (this.logLock) {
                logMessages = new HashSet<>(this.logs);
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
                params.put("configtype", "pluginremove");
                params.put("plugin", this.pluginID);
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));
            }
            this.running = false;
            this.alive = false;
        }
    }

    public static boolean isBadPacketSending() {
        return badPacketSending;
    }
    public static void setBadPacketSending(boolean badPacketSending) {
        APIController.badPacketSending = badPacketSending;
    }
}
