package activitystreamer.server;

import java.io.*;
import java.net.*;
import java.util.*;

import activitystreamer.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;
	private static ArrayList<Connection> serverconnections;
	private static boolean term = false;
	private static Listener listener;
	private static JSONObject usersinfo;
	private static JSONObject thisSever;
	private static HashMap<String,Connection> connectionMap;
	private static HashMap<String,Integer> countMap;
	private static HashMap<Connection,String> loginMap;

	protected static Control control = null;

	//---------------
	private final int REDIRECT_LIMIT = 2;
	private static ArrayList<JSONObject> serverlist;
	private static boolean redirect = false;
	private static String redirectHost = null;
	private static String redirectPort = null;


	public static Control getInstance() {
		if (control == null) {
			control = new Control();
		}
		return control;
	}

	public Control() {
		// initialize the connections array
		connections = new ArrayList<Connection>();
		serverconnections = new ArrayList<Connection>();
		connectionMap = new HashMap<String,Connection>();
		countMap = new HashMap<String,Integer>();
		usersinfo = new JSONObject();
		usersinfo.put("command", "usersinfo");
		loginMap = new HashMap<Connection,String>();
		serverlist = new ArrayList<JSONObject>();
		JSONObject thisServer = new JSONObject();
		thisServer.put("id", Server.getId());
		if (thisServer != null) {
			serverlist.add(thisServer);
		}
		// start a listener
		try {
			listener = new Listener();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: " + e1);
			System.exit(-1);
		}
	}

	public void initiateConnection() {
		// make a connection to another server if remote hostname is supplied
		if (Settings.getRemoteHostname() != null) {
			try {
				outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
	}

	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	public synchronized boolean process(Connection con, String msg) {
		try {
			JSONParser parser = new JSONParser();
			JSONObject message = (JSONObject) parser.parse(msg);
//-------------------------------Invalid message : No command
			String match = "command";

			if (!msg.contains(match)) {
				String ms = "the received message did not contain a command";
				return sendInvalidMessage(con,ms);
			}
			log.debug("received a "+message.get("command")+" from "+
					con.getSocket().getInetAddress()+"/"+con.getSocket().getLocalPort());
			if (message.get("command").equals("INVALID_MESSAGE")) {
				log.warn(message.get("info"));
			}

//-------------------------------receive AUTHENTICATION_FAIL (server initiation reply)
			if (message.get("command").equals("AUTHENTICATION_FAIL")) {
				log.warn(message.get("info"));
				return true;
			}

//-------------------------------receive AUTHENTICATE (other servers to this server)
			else if (message.get("command").equals("AUTHENTICATE")) {
				if (message.get("secret")==null) {
					String ms = "the received message did not contain a secret";
					return sendInvalidMessage(con,ms);

				} else if (message.size() != 2) {
					String ms = "incorrect message";
					return sendInvalidMessage(con,ms);
				} else if (message.get("secret").equals(Settings.getSecret())) {
					serverconnections.add(con);
					sentmessage(usersinfo,con);
					return false;
				} else {
					JSONObject authenticateFail = new JSONObject();
					authenticateFail.put("command", "AUTHENTICATION_FAIL");
					authenticateFail.put("info", "the supplied secret is incorrect: " + message.get("secret"));
					sentmessage(authenticateFail, con);
					sentmessage(usersinfo,con);
					return true;
				}
			}
//--------------------------Synchronize the userinfo for new server
			if (message.get("command").equals("usersinfo")) {
				usersinfo=new JSONObject();
				usersinfo = message;
			}

//--------------------------------ACTIVITY_MESSAGE from client
			else if (message.get("command").equals("ACTIVITY_MESSAGE")) {
				if (message.get("username") == null || message.get("activity") == null
						|| message.size() != 4) {
					String ms = "incorrect message";
					return sendInvalidMessage(con, ms);
				}
				if(!(loginMap.containsKey(con))){
					JSONObject fail = new JSONObject();
					fail.put("command", "AUTHENTICATION_FAIL");
					fail.put("info", "must send a LOGIN message first");
					sentmessage(fail, con);
					return true;
				}
				if(usersinfo.get(message.get("username"))!=null){
					if((loginMap.containsValue(message.get("username"))) &&
							(usersinfo.get(message.get("username")).equals(message.get("secret")))
							|| message.get("username").equals("anonymous")){
						JSONObject broad = new JSONObject();
						JSONObject act = (JSONObject) message.get("activity");
						act.put("authenticated_user", message.get("username"));
						broad.put("command", "ACTIVITY_BROADCAST");
						broad.put("activity", act);
						String activity = broad.toJSONString();
						broadcast(activity, connections);
						return false;
					}
				}
				if(loginMap.containsValue(message.get("username"))&&
						message.get("username").equals("anonymous")){
					JSONObject broad = new JSONObject();
					JSONObject act = (JSONObject) message.get("activity");
					act.put("authenticated_user", message.get("username"));
					broad.put("command", "ACTIVITY_BROADCAST");
					broad.put("activity", act);
					String activity = broad.toJSONString();
					broadcast(activity, connections);
					return false;
				}
				else {
					JSONObject fail = new JSONObject();
					fail.put("command", "AUTHENTICATION_FAIL");
					fail.put("info", "username and/or secret is incorrect");

					sentmessage(fail, con);
					return true;
				}
			}
//--------------------------------ACTIVITY_BROADCAST from sever
			else if (message.get("command").equals("ACTIVITY_BROADCAST")) {
				if(!serverconnections.contains(con)){
					String ms = "received ACTIVITY_BROADCAST from an unauthenticated server";
					return sendInvalidMessage(con, ms);
				}
				String activity = message.toJSONString();
				sendToOthers(con, activity, connections);
				return false;

			}

// --------------------------------Accept Announcement
			else if (message.get("command").equals("SERVER_ANNOUNCE")) {
//				log.info(message.toJSONString());
				if (message.size() != 5) {
					String ms = "incorrect message";
					return sendInvalidMessage(con, ms);
				}
				if (!serverconnections.contains(con)) {
					String ms = "received SERVER_ANNOUNCE from an unauthenticated server";
					return sendInvalidMessage(con, ms);
				}
				log.debug("received announcement from " + message.get("id") + " load " +
						message.get("load") + " at " +
						message.get("hostname") + ":" + message.get("port"));
				if (serverlist != null) {
					boolean flag = true;
					for (JSONObject server : serverlist) {
						if (server.get("id").equals(message.get("id"))) {
							server.put("load", message.get("load"));
							flag = false;
							break;

						}
					}
					if (flag) {
						JSONObject newServer = new JSONObject();
						newServer.put("id", message.get("id"));
						newServer.put("load", message.get("load"));
						newServer.put("hostname", message.get("hostname"));
						newServer.put("port", message.get("port"));
						if (newServer != null) {
							serverlist.add(newServer);
						}
					}
					if (!redirect) {
						if (connections.size() - serverconnections.size() - Integer.parseInt(message.get("load").toString()) > REDIRECT_LIMIT) {
							redirect = true;
							redirectHost = message.get("hostname").toString();
							redirectPort = message.get("port").toString();
						}
					}
				}

				String announce = message.toJSONString();
				sendToOthers(con, announce, serverconnections);
				return false;
			}

// --------------------------------LOGIN
			else if (message.get("command").equals("LOGIN")) {
				String username = message.get("username").toString();
				if (message.get("username") == null ) {
					String ms = "incorrect message";
					return sendInvalidMessage(con, ms);
				}

				if (usersinfo.get(message.get("username")) == null
						&& !(message.get("username").equals("anonymous"))) {
					JSONObject loginFailed = new JSONObject();
					loginFailed.put("command", "LOGIN_FAILED");
					loginFailed.put("info", "user "+message.get("username")+" is not registered");
					sentmessage(loginFailed, con);
					return true;
				} else {
					if ((message.get("username").equals("anonymous")) ||
							(usersinfo.get(message.get("username")).equals(message.get("secret")))) {
						//LOGIN SUCCESS, NO REDIRECT
						if (!redirect) {
							JSONObject loginSuccess = new JSONObject();
							loginSuccess.put("command", "LOGIN_SUCCESS");
							loginSuccess.put("info", "logged in as user: " + username);
							sentmessage(loginSuccess, con);
							loginMap.put(con,message.get("username").toString());
							return false;
						} else {
							//REDIRECT
							JSONObject redirectCommand = new JSONObject();
							redirectCommand.put("command", "REDIRECT");
							redirectCommand.put("hostname", redirectHost);
							redirectCommand.put("port", redirectPort);
							sentmessage(redirectCommand, con);
							redirect = false;
							log.info(message.get("username") + " has been redirected to: "
									+ redirectHost + ":" + redirectPort);
							return true;
						}
					} else {
						//LOGIN FAILED
						JSONObject loginFailed = new JSONObject();
						loginFailed.put("command", "LOGIN_FAILED");
						loginFailed.put("info", "wrong secret for user "+message.get("username"));
						sentmessage(loginFailed, con);
						return true;
					}
				}

			}
// --------------------------------LOGOUT
			else if (message.get("command").equals("LOGOUT")) {
				loginMap.remove(con);
				return true;
			}
//-------------------------------receive REGISTER (Client to this server)
			else if (message.get("command").equals("REGISTER")) {
				if (message.get("secret") == null ||
						message.get("username") == null || message.size() != 3) {
					String ms = "incorrect message";
					return sendInvalidMessage(con, ms);
				}
//----------------Invalid message: already logged in on this connection
				if (loginMap.containsValue(con)) {
					loginMap.remove(con);
					String ms = "received "+message.get("command")+" from a client " +
							"that has already logged in as "+message.get("username");
					return sendInvalidMessage(con, ms);
				}
				//check if username has been registered with a different secret at local server
				if ((usersinfo.get(message.get("username")) != null)) {
					JSONObject registerFail = new JSONObject();
					registerFail.put("command", "REGISTER_FAILED");
					registerFail.put("info", message.get("username") + " is already registered with the system");
					sentmessage(registerFail, con);
					return true;
				}
				if ((usersinfo.get(message.get("username")) == null) && serverconnections.size() == 0) {
					JSONObject registerSuccess = new JSONObject();
					registerSuccess.put("command", "REGISTER_SUCCESS");
					registerSuccess.put("info", "register success for " + message.get("username"));
					sentmessage(registerSuccess, con);
					usersinfo.put(message.get("username"), message.get("secret"));
					return false;

				}



//-----------broadcast lock-request to other servers to check if username has been taken or not
				else {
					JSONObject lock_request = new JSONObject();
					lock_request.put("command", "LOCK_REQUEST");
					lock_request.put("username", message.get("username"));
					lock_request.put("secret", message.get("secret"));
					String lockrequest = lock_request.toJSONString();
					//save the cli info and let the cli wait for the approval from other servers
					connectionMap.put(message.get("username").toString(), con);
					countMap.put(message.get("username").toString(), 0);
					usersinfo.put(message.get("username"), message.get("secret"));
					broadcast(lockrequest, serverconnections);
					return false;
				}
			}

//-------------------------------receive LOCK_REQUEST from other servers
			else if (message.get("command").equals("LOCK_REQUEST")) {

				if (!serverconnections.contains(con)) {
					String ms = "received LOCK_REQUEST from an unauthenticated server";
					return sendInvalidMessage(con, ms);
				}
				if (message.get("secret") == null ||
						message.get("username") == null || message.size() != 3) {
					String ms = "incorrect message";
					return sendInvalidMessage(con, ms);
				}
				sendToOthers(con, msg, serverconnections);
				//deny the request if username has been registered with a different secret
				if (usersinfo.get(message.get("username")) != null) {
					JSONObject lock_denied = new JSONObject();
					lock_denied.put("command", "LOCK_DENIED");
					lock_denied.put("username", message.get("username"));
					lock_denied.put("secret", message.get("secret"));
					String lockdeny = lock_denied.toJSONString();
					broadcast(lockdeny, serverconnections);
					usersinfo.remove(message.get("username"));
					return false;
				}

				//------------allow the request if no match for username has been found in the server
				if (usersinfo.get(message.get("username")) == null) {
					JSONObject lock_allowed = new JSONObject();
					lock_allowed.put("command", "LOCK_ALLOWED");
					lock_allowed.put("username", message.get("username"));
					lock_allowed.put("secret", message.get("secret"));
					String lockallow = lock_allowed.toJSONString();
					//add userinfo into the registration list
					broadcast(lockallow, serverconnections);
					usersinfo.put(message.get("username"), message.get("secret"));
					log.info("Register: " + "username " + "\"" + message.get("username") + "\"" +
							" has been added into storage");
					return false;
				}
			}
//---------------------get registration removed if received lock_denied
			else if (message.get("command").equals("LOCK_DENIED")) {
				if (message.get("secret") == null ||
						message.get("username") == null || message.size() != 3) {
					String ms = "incorrect message";
					return sendInvalidMessage(con, ms);
				}
				if (!serverconnections.contains(con)) {
					String ms = "received LOCK_DENIED from an unauthenticated server";
					return sendInvalidMessage(con, ms);
				}
				usersinfo.remove(message.get("username"), message.get("secret"));
				if (connectionMap.get(message.get("username")) != null) {
					JSONObject registerFail = new JSONObject();
					registerFail.put("command", "REGISTER_FAILED");
					registerFail.put("info", message.get("username") + " is already registered with the system");
					sentmessage(registerFail, connectionMap.get(message.get("username")));

					log.info("REGISTER_FAILED for: " + "\"" + message.get("username") + "\"");
					return true;
				} else {
					sendToOthers(con, msg, serverconnections);
					log.info("Register: " + "username \"" + message.get("username") + "\" has been removed");
					return false;
				}
			}

//-----------receive lock allowed from other server and client is not registering through this server
			else if (message.get("command").equals("LOCK_ALLOWED")) {
				if (message.get("secret") == null ||
						message.get("username") == null || message.size() != 3) {
					String ms = "incorrect message";
					return sendInvalidMessage(con, ms);
				}
				if (!serverconnections.contains(con)) {
					String ms = "received LOCK_ALLOWED from an unauthenticated server";
					return sendInvalidMessage(con, ms);
				}

				if (countMap.get(message.get("username")) == null) {
					sendToOthers(con, msg, serverconnections);
					return false;
				} else {
					int i = countMap.get(message.get("username")) + 1;
					if (i == serverlist.size() - 1) {
						JSONObject registerSuccess = new JSONObject();
						registerSuccess.put("command", "REGISTER_SUCCESS");
						registerSuccess.put("info", "register success for " + message.get("username"));
						sentmessage(registerSuccess, connectionMap.get(message.get("username")));
						connectionMap.remove(message.get("username"));
						countMap.remove(message.get("username"));
						log.info("REGISTER_SUCCESS for: " + message.get("username"));
						return false;
					} else {
						countMap.replace(message.get("username").toString(), i);

						return false;
					}
				}
			}
//--------------------------------UNKNOWN COMMANDS
			else {
				String ms = "the message contained an unknown command:"+message.get("command");
				return sendInvalidMessage(con, ms);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con) {
		if (!term) {
			connections.remove(con);
			if (serverconnections.contains(con)) {
				serverconnections.remove(con);
			}
			if (loginMap.containsKey(con)) {
				loginMap.remove(con);
			}
		}

	}


	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException {
		log.debug("incomming connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
//		c.start();
		return c;

	}

	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
//		c.start();

		JSONObject outgo = new JSONObject();
		outgo.put("command", "AUTHENTICATE");
		outgo.put("secret", Settings.getSecret());

		sentmessage(outgo, c);

		connections.add(c);
		serverconnections.add(c);
		return c;

	}

	@Override
	public void run() {
		log.info("using activity interval of " + Settings.getActivityInterval() + " milliseconds");
		while (!term) {
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}

			if (!term) {
				term = doActivity();
			}

		}
		log.info("closing " + connections.size() + " connections");
		// clean up
		for (Connection connection : connections) {
			connection.closeCon();
		}
		listener.setTerm(true);
	}

	public boolean doActivity() {
//--------------------------------send SERVER_ANNOUNCE between severs
		try {
			if (serverconnections.size() != 0) {
				JSONObject announce = new JSONObject();
				announce.put("command", "SERVER_ANNOUNCE");
				announce.put("id", Server.getId());
				announce.put("load", connections.size() - serverconnections.size());
				announce.put("hostname", Settings.getLocalHostname());
				announce.put("port", Settings.getLocalPort());
				String announcement = announce.toJSONString();
				broadcast(announcement, serverconnections);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public final void setTerm(boolean t) {
		term = t;
	}

	public final ArrayList<Connection> getConnections() {
		return connections;
	}

	public void sentmessage(JSONObject json, Connection con) throws IOException {
		Socket s = con.getSocket();
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(s.getOutputStream(),
						"UTF-8"));
		writer.write(json.toJSONString() + "\n");
		writer.flush();
	}


	public synchronized void broadcast(String msg, ArrayList<Connection> connections) throws IOException {
		for (Connection con : connections) {
			Socket s = con.getSocket();
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(s.getOutputStream(),
							"UTF-8"));
			writer.write(msg + "\n");
			writer.flush();

		}
	}

	public void sendToOthers(Connection con, String msg, ArrayList<Connection> connections) throws IOException {
		int index = connections.indexOf(con);
		ArrayList<Connection> temp = new ArrayList<Connection>();
		for (int x = 0; x < connections.size(); x++) {
			if (x != index) {
				temp.add(connections.get(x));
			}
		}
		broadcast(msg, temp);
	}

	public boolean sendInvalidMessage(Connection con, String msg)throws IOException{
		JSONObject invalid = new JSONObject();
		invalid.put("command", "INVALID_MESSAGE");
		invalid.put("info", msg);
		sentmessage(invalid, con);
		return true;
	}

}
