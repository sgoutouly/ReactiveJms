package com.sylvain.reactivejms.sse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;
import rx.subjects.PublishSubject;

/**
 * 
 * 
 * @author sylvain
 */
public final class JmsSseServer {

    public static void main(final String[] args) {

        Map<String, HttpServerResponse<ServerSentEvent>> clients = new HashMap<>();
    	PublishSubject<String> dataStream = PublishSubject.create();
    	
    	HttpServer<ByteBuf, ByteBuf> server;

    	server = HttpServer.newServer(9000)
           .enableWireLogging("sse-server", LogLevel.INFO)
           .start((req, resp) -> {
        	   
        	   // SSE endPoint
        	   if (req.getUri().startsWith("/connect")) {
            	   	HttpServerResponse<ServerSentEvent> sseStream = resp.transformToServerSentEvents();
            	   	
            	   	clients.put(sseStream.toString(), sseStream);
            	   	System.out.println("Nb clients connectés : " +  (clients.size() + 1));
            	    
            	   	return sseStream.writeAndFlushOnEach(dataStream
            	   		.onBackpressureBuffer(10)
                		.map(m -> ServerSentEvent.withData(m + "\n")));
        	    }
        	   // Pusht data to SSE endPoint
        	    else if(req.getUri().startsWith("/push")) {
        	    	dataStream.onNext("One message ! " + UUID.randomUUID().toString());
        	    	return resp.writeString(Observable.just("Data sent !"));
        	    }
        	   // Index
        	    else if(req.getUri().startsWith("/")) {
        	    	StringBuffer html = new StringBuffer()
        	    		.append("<script>").append("\n")
    	    			.append("var sse = new EventSource(\"http://localhost:9000/connect\");").append("\n")
    	    			.append("sse.onmessage = function(event) { console.log(\"SSE message : \" + event.data); };").append("\n")
    	    			.append("sse.onopen = function() { console.log(\"SSE connected !\"); };").append("\n")
    	    			.append("</script>");
        	    	return resp.writeString(Observable.just(html.toString()));
        	    }
        	   // Not Found handler
        	    else {
        	    	resp.setStatus(HttpResponseStatus.NOT_FOUND);
        	    	return resp.writeAndFlushOnEach(Observable.empty());
        	    }
        	   
           	}
          );
        
        server.awaitShutdown();
        
    }
}
