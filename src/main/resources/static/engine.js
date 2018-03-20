 /** */
 var stompClient;
 /** */
 var subscription;
 /** */
 var sse;

  /**
   *
   */
  function init() {

    //  sse = new EventSource("http://localhost:8080/sse/connect");
    sse = new EventSource("http://localhost:9000/connect");
    writeToScreen("SSE : " + "Test");
    sse.onmessage = function(event) {
      writeToScreen("SSE : " + event.data);
    };


    //
   /* websocket = new WebSocket("ws://localhost:8080/servicesSocket");
    websocket.onclose = function(evt) { writeToScreen("DECONNECTE !"); };
    websocket.onerror = function(evt) { writeToScreen('<span style="color: red;">ERROR:</span> ' + evt.data) };

    //
    document.getElementById("sendToChatButton").addEventListener("click",
      function() {
        doSendToChat(document.getElementById('message').value);
        document.getElementById('message').value = '';
      }
      ,false
    );

    //
    document.getElementById("sendToAppButton").addEventListener("click",
      function() {
        doSendToApplication(document.getElementById('message').value);
        document.getElementById('message').value = '';
      }
      ,false
    );

    //
    stompClient = Stomp.over(websocket);
    stompClient.connect({},
      function() {
        writeToScreen("CONNECTE !");
        subscribeToChat();
      },
      function(error) {
        writeToScreen("ERREUR DE CONNEXION : " + error.headers.message);
      });
      */
  }

  /**
   *
   */
  function subscribeToChat() {
    subscription = stompClient.subscribe("/topic/chat",
      function(message) {
        writeToScreen('<span style="color: blue;">RECU: ' + message.body + '</span>')
      });
  }

  /**
   *
   */
  function doSendToChat(message) {
    writeToScreen("ENVOYE AUX ABONNES: " + message);
    stompClient.send("/topic/chat", {}, message);
  }

  /**
   *
   */
  function doSendToApplication(message) {
    writeToScreen("ENVOYE A L'APPLICATION: " + message);
    stompClient.send("/app/chat", {}, JSON.stringify({"body": message}));
  }

  /**
   *
   */
  function writeToScreen(message) {
    var pre = document.createElement("p");
    pre.style.wordWrap = "break-word";
    pre.innerHTML = message;
    document.getElementById("output").appendChild(pre);
  }

  window.addEventListener("load", init, false);