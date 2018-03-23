package com.sylvain.reactivejms.sse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;
import rx.subjects.PublishSubject;

/**
 * 
 * @author sylvain
 */
public final class JmsSseServer {

    private static Channel channel; 
    private static Connection connection;
    
    
    public static void main(final String[] args) throws IOException, TimeoutException, UnsupportedEncodingException, InterruptedException  {
       
    	PublishSubject<String> dataStream = PublishSubject.create();
        
    	ExecutorService rabbitExecutor = Executors.newSingleThreadExecutor();
        
        Consumer consumer = new DefaultConsumer(channel) {
        	@Override
        	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        		String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                dataStream.onNext("One message ! " + UUID.randomUUID().toString());
        	}
        };

        
        connect();
        
        rabbitExecutor.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println("Démarrage du listener ...");
                
                try {
                    channel.basicConsume("hello", true, consumer);
                } 
                catch (IOException e) {  }

                System.out.println("... listener démarré !");           
            }
        });
        
        rabbitExecutor.shutdown();


        Map<String, HttpServerResponse<ServerSentEvent>> clients = new HashMap<>();
        
        HttpServer.newServer(9000).start((req, resp) -> {
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
                //dataStream.onNext("One message ! " + UUID.randomUUID().toString());
                try {
                    System.out.println("Envoi d'un message !");
                    channel.basicPublish("", "hello", null, ("One message ! " + UUID.randomUUID().toString()).getBytes());
                } 
                catch (IOException e) {}
                
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
           
        }).awaitShutdown();
        
    }
    
    public static void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
    }
}
