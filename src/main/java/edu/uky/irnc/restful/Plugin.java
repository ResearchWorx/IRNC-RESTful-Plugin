package edu.uky.irnc.restful;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import edu.uky.irnc.restful.controllers.APIController;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.OutputStream;
import java.net.URI;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;

@AutoService(CPlugin.class)
public class Plugin extends CPlugin {
    private static final String BASE_URI = "http://[::]:32001/";
    private HttpServer server;

    public void start() {
        setExec(new Executor(this));
        server = startServer();
        logger.info("Server up");
    }

    @Override
    public void cleanUp() {
        server.shutdownNow();
        for (APIController.QueueListener listener : APIController.listeners.values())
            listener.close();
        logger.info("Server down");
    }

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    private HttpServer startServer() {
        System.setProperty("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
        System.setProperty("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "WARNING");
        final OutputStream nullOutputStream = new OutputStream() { @Override public void write(int b) { } };
        Logger.getLogger("").addHandler(new ConsoleHandler() {{ setOutputStream(nullOutputStream); }});

        final ResourceConfig rc = new ResourceConfig()
                .register(APIController.class);

        APIController.setPlugin(this);
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    public static void main(String[] args) {
        System.out.println("This is not meant to be used outside of the Cresco framework.");
    }
}
