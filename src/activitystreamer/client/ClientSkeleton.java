package activitystreamer.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.Socket;
import java.net.SocketException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonObject;

import activitystreamer.Client;
import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
    private static final Logger log = LogManager.getLogger();
    private static ClientSkeleton clientSolution;
    private TextFrame textFrame;
    private BufferedWriter writer;
    private BufferedReader reader;
    private Socket s = null;
    private JSONParser parser = new JSONParser();

    public static ClientSkeleton getInstance(int situation) {
        if (clientSolution == null) {
            clientSolution = new ClientSkeleton(situation);
        }
        return clientSolution;
    }

    public void connect() {
        try {
            this.s = new Socket(Settings.getRemoteHostname(),
                    Settings.getRemotePort());
            // this.s = new Socket("sunrise.cis.unimelb.edu.au",3780);
            // this.s = new Socket("localhost",4444);
            writer = new BufferedWriter(new OutputStreamWriter(
                    this.s.getOutputStream(), "UTF-8"));
            reader = new BufferedReader(
                    new InputStreamReader(s.getInputStream(), "UTF-8"));
            log.info("Connection with server established");
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public ClientSkeleton(int situation) {
        connect();
        if (situation == 1) {
            String secret = Settings.nextSecret();
            sendActivityObject(
                    BuildReg(Settings.getUsername(), secret));
            log.info("you register with username"
                    + Settings.getUsername() + "and your secret is: "
                    + secret);
        }
        if (situation == 3) {
            sendActivityObject(BuildLog(Settings.getUsername(),
                    Settings.getSecret()));
        }
        if (situation == 4) {
            sendActivityObject(BuildLogAnonymous("anonymous"));
        }
        textFrame = new TextFrame();
        start(); // start listening for message from server

    }

    public void sendActivityObject(String msg) {
        try {
            JSONObject obj = (JSONObject) parser.parse(msg);
            writer.write(obj.toString() + "\n");
            writer.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.info("The connection has been closed by server");
            System.exit(-1);
        } catch (ParseException e1) {
            e1.printStackTrace();
            log.error(
                    "invalid JSON object entered into input text field, data not sent");
        }
    }

    public void disconnect() {
        try {
            this.s.close();
        } catch (SocketException e) {
            System.out
                    .println("Socket closed because the user typed exit");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public String BuildBroadcastMessage(String msg) {
        return "{\"command\" : \"ACTIVITY_MESSAGE\", \"username\" : \""
                + Settings.getUsername() + "\", \"secret\" : \""
                + Settings.getSecret()
                + "\", \"activity\" : { \"message\":\"" + msg + "\" }}";
    }

    public String BuildLogout() {
        return "{\"command\" : \"LOGOUT\"}";
    }

    public String BuildReg(String uname, String secret) {
        return "{\"command\" : \"REGISTER\", \"username\" : \"" + uname
                + "\", \"secret\" : \"" + secret + "\"}";
    }

    public String BuildLog(String uname, String secret) {
        return "{\"command\" : \"LOGIN\", \"username\" : \"" + uname
                + "\", \"secret\" : \"" + secret + "\"}";
    }

    public String BuildLogAnonymous(String uname) {
        return "{\"command\" : \"LOGIN\", \"username\" : \"" + uname
                + "\"}";
    }

    // 持续监听消息
    public void run() {
        try {
            String msg = null;
            log.info("starting listening");
            while ((msg = reader.readLine()) != null) {
                // Print the messages to the console
                System.out.println(
                        "Here is the message from server: " + msg);
                JSONParser parser = new JSONParser();
                JSONObject resultFromServer = (JSONObject) parser
                        .parse(msg); // 解析它的消息
                // 这里判断如果是redirect的消息 就自己去重新连接一个服务器
                if (resultFromServer.get("command").equals("REDIRECT")) {
                    String rh = resultFromServer.get("hostname")
                            .toString();
                    int rp = Integer.parseInt(
                            resultFromServer.get("port").toString());
                    Settings.setRemoteHostname(rh);
                    Settings.setRemotePort(rp);
                    // when redirect, if the Setting.secret is null means it was an anonymous login
                    if (Settings.getSecret() == null) {
                       connect();
                        sendActivityObject(BuildLogAnonymous("anonymous"));
                        new Thread() {
                            public void run() {
                                String msg = null;
                                log.info("starting listening");
                                try {
                                    while ((msg = reader
                                            .readLine()) != null) {
                                        // Print the messages to the console
                                        System.out.println(
                                                "Here is the message from server: "
                                                        + msg);
                                        JSONParser parser = new JSONParser();
                                        JSONObject resultFromServer = (JSONObject) parser
                                                .parse(msg); // parse the message
                                        textFrame.setOutputText(
                                                resultFromServer);
                                    }
                                } catch (IOException | ParseException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                        }.start();
                        Thread.currentThread().interrupt();
                    } else {
                         connect();
                         sendActivityObject(BuildLog(Settings.getUsername(),
                                 Settings.getSecret()));
                         new Thread() {
                             public void run() {
                                 String msg = null;
                                 log.info("starting listening");
                                 try {
                                     while ((msg = reader
                                             .readLine()) != null) {
                                         // Print the messages to the console
                                         System.out.println(
                                                 "Here is the message from server: "
                                                         + msg);
                                         JSONParser parser = new JSONParser();
                                         JSONObject resultFromServer = (JSONObject) parser
                                                 .parse(msg); 
                                         textFrame.setOutputText(
                                                 resultFromServer);
                                     }
                                 } catch (IOException | ParseException e) {
                                     // TODO Auto-generated catch block
                                     e.printStackTrace();
                                 }
                             }
                         }.start();
                         //start a new thread for listening and kill the currentThread
                         Thread.currentThread().interrupt();
                    }
                } else
                    textFrame.setOutputText(resultFromServer);
            }
        } catch (SocketException e) {
            e.printStackTrace();
            log.info("Socket closed because the user typed disconnect");
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
