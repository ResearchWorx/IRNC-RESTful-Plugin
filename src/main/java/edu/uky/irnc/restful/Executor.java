package edu.uky.irnc.restful;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;
import edu.uky.irnc.restful.controllers.APIController;

import java.util.HashMap;
import java.util.Map;

class Executor extends CExecutor {
    private final CLogger logger;

    Executor(CPlugin plugin) {
        super(plugin);
        logger = new CLogger(Executor.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                plugin.getPluginID(), CLogger.Level.Trace);
    }

    @Override
    public MsgEvent processExec(MsgEvent msg) {
        logger.debug("Processing EXEC message: {}", msg.getParams());
        switch (msg.getParam("cmd")) {
            case "delete_exchange":
                logger.info("Command 'delete_exchange' received");
                logger.trace("Grabbing relevant QueueListener (if one exists)");
                logger.debug("exchange: {}", msg.getParam("exchange"));
                APIController.QueueListener listenerToDelete = APIController.listeners.get(msg.getParam("exchange"));
                if (listenerToDelete != null) {
                    logger.trace("Found relevant QueueListener, issuing done() command");
                    listenerToDelete.done();
                } else
                    logger.debug("No QueueListener found matching given exchange");
                Map<String, String> params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("dst_region", plugin.getRegion());
                params.put("dst_agent", plugin.getAgent());
                params.put("configtype", "pluginremove");
                params.put("plugin", msg.getParam("src_plugin"));
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(),
                        plugin.getAgent(), plugin.getPluginID(), params));
                break;
            case "execution_log":
                logger.info("Command 'execution_log' received");
                logger.trace("Grabbing relevant QueueListener (if one exists)");
                logger.debug("exchange: {}", msg.getParam("exchange"));
                APIController.QueueListener listenerForLog = APIController.listeners.get(msg.getParam("exchange"));
                if (listenerForLog != null) {
                    logger.trace("Found relevant QueueListener, adding log");
                    logger.debug("log: {}", msg.getParam("log"));
                    listenerForLog.log(msg.getParam("ts"), msg.getParam("log"));
                } else
                    logger.debug("No QueueListener found matching given exchange");
                break;
            default:
                logger.debug("Unknown cmd: {}", msg.getParam("cmd"));
        }
        return null;
    }
}
