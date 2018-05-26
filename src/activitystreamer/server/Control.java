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
	private static HashMap<String, Connection> connectionMap;
	private static HashMap<String, Integer> countMap;

	private static HashMap<Connection, String> loginMap;
	private static HashMap<String, Long> registerTime;
	private static HashMap<String, JSONObject> allServerLoad;
	private static HashMap<Connection, Integer> connectionRemotePort;


	public static Connection nextRootNode;

	protected static Control control = null;

	private static boolean freeze = false;
	private static int triggerParent = 0;
	public static Connection parent;
	private static HashMap<Connection, Integer> child;
	//---------------
	private static HashMap<String, JSONObject> SERVERLIST;
	private static JSONObject reconnectInfo;
	private static JSONObject reconnectUse;
	private static JSONObject rootReconnect;

	private static void setReconnectInfo(JSONObject obj) {
		reconnectInfo = obj;
	}


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
		connectionMap = new HashMap<String, Connection>();
		countMap = new HashMap<String, Integer>();
		usersinfo = new JSONObject();
		usersinfo.put("command", "usersinfo");
		reconnectInfo = new JSONObject();
		reconnectInfo.put("command", "RECONNECT_INFO");
		loginMap = new HashMap<Connection, String>();

		SERVERLIST = new HashMap<String, JSONObject>();
		JSONObject thisServer = new JSONObject();
		allServerLoad = new HashMap<String, JSONObject>();
		connectionRemotePort = new HashMap<Connection, Integer>();
		child = new HashMap<Connection, Integer>();
		rootReconnect = new JSONObject();
		registerTime = new HashMap<String, Long>();


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
//			log.warn("============" + msg);
			JSONParser parser = new JSONParser();
			JSONObject message = (JSONObject) parser.parse(msg);
//-------------------------------Invalid message : No command
			String match = "command";
			if (!msg.contains(match)) {
				String ms = "the received message did not contain a command";
				return sendInvalidMessage(con, ms);
			}
//			log.debug("received a " + message.get("command") + " from " +
//					con.getSocket().getInetAddress() + "/" + con.getSocket().getPort());
			if (message.get("command").equals("INVALID_MESSAGE")) {
				log.warn(message.get("info"));
			}
//-------------判断register超时
			registerTimeout();
//--------------------------
			switch (message.get("command").toString()) {
				case "AUTHENTICATION_FAIL":
					log.warn(message.get("info"));
					return true;

				case "AUTHENTICATE":
					return authenticate(message, con);
				case "usersinfo":
					usersinfo = new JSONObject();
					usersinfo = message;
					parent = con;
					return false;
				case "ACTIVITY_MESSAGE":
					return acitivityMessage(message, con);
				case "ACTIVITY_BROADCAST":
					return acitivityBroadcast(message, con);
				case "SERVER_ANNOUNCE":
					if (con == parent) {
						triggerParent = 0;
					}
					for (int i = 0; i < child.size(); i++) {
						child.replace(con, 0);
					}
					return serverAnnounce(message, con);
				case "LOGIN":

					return login(message, con);
				case "LOGOUT":
//-----------------load change
					sendServerUpdate("SERVER_UPDATE", con);
					loginMap.remove(con);
					return true;
				case "REGISTER":
					return register(message, con);
				case "LOCK_REQUEST":
					return lockRequest(msg, message, con);
				case "LOCK_DENIED":
					return lockDenied(msg, message, con);
				case "LOCK_ALLOWED":
					return lockAllowed(msg, message, con);
				case "RECONNECT_INFO":
					return getreconnectInfo(message);
				case "RE_AUTHENTICATE":
					return reAuthenticate(message, con);
				case "SERVER_UPDATE":
					return updateToOther(message, con);
				case "REMOTE_PORT":
					return romotePort(message, con);
				case "RECONNECT_SUCCESS":
					return reconnectSuccess(message, con);
				//以上正确 重连接后传给子节点

				case "DELETE_FATHER":
					if (reconnectUse.get("father") != null) {
						reconnectUse.remove("father");
					}
					return false;

				case "ROOT_RECONNECT":
					rootReconnect.clear();
					rootReconnect.put("info", message.get("ROOT_RECONNECT"));
					return false;

				case "LOAD_REQUEST":
					return broadcastLoadRequest(message, con);

				case "TARGET_SEND_LOAD":
					sentLoadOrNot(message, con);
					return false;
				default:
					String ms = "the message contained an unknown command:" + message.get("command");
					return sendInvalidMessage(con, ms);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean sendInvalidMessage(Connection con, String msg) throws IOException {
		JSONObject invalid = new JSONObject();
		invalid.put("command", "INVALID_MESSAGE");
		invalid.put("info", msg);
		sentmessage(invalid, con);
		return true;
	}

	public boolean acitivityMessage(JSONObject message, Connection con) throws IOException {
		if (message.get("username") == null || message.get("activity") == null
				|| message.size() != 4) {
			String ms = "incorrect message";
			return sendInvalidMessage(con, ms);
		}
		if (!(loginMap.containsKey(con))) {
			JSONObject fail = new JSONObject();
			fail.put("command", "AUTHENTICATION_FAIL");
			fail.put("info", "must send a LOGIN message first");
			sentmessage(fail, con);
			return true;
		}
		if (usersinfo.get(message.get("username")) != null) {
			if ((loginMap.containsValue(message.get("username"))) &&
					(usersinfo.get(message.get("username")).equals(message.get("secret")))
					|| message.get("username").equals("anonymous")) {
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
		if (loginMap.containsValue(message.get("username")) &&
				message.get("username").equals("anonymous")) {
			JSONObject broad = new JSONObject();
			JSONObject act = (JSONObject) message.get("activity");
			act.put("authenticated_user", message.get("username"));
			broad.put("command", "ACTIVITY_BROADCAST");
			broad.put("activity", act);
			String activity = broad.toJSONString();
			broadcast(activity, connections);
			return false;
		} else {
			JSONObject fail = new JSONObject();
			fail.put("command", "AUTHENTICATION_FAIL");
			fail.put("info", "username and/or secret is incorrect");

			sentmessage(fail, con);
			return true;
		}
	}

	public boolean acitivityBroadcast(JSONObject message, Connection con) throws IOException {
		if (!serverconnections.contains(con)) {
			String ms = "received ACTIVITY_BROADCAST from an unauthenticated server";
			return sendInvalidMessage(con, ms);
		}
		String activity = message.toJSONString();
		sendToOthers(con, activity, connections);
		return false;
	}

	public boolean serverAnnounce(JSONObject message, Connection con) throws IOException {
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


		redirection(message);

		String announce = message.toJSONString();
		sendToOthers(con, announce, serverconnections);
		return false;
	}

	public boolean login(JSONObject message, Connection con) throws IOException {
		String username = message.get("username").toString();
		if (message.get("username") == null) {
			String ms = "incorrect message";
			return sendInvalidMessage(con, ms);
		}

		if (usersinfo.get(message.get("username")) == null
				&& !(message.get("username").equals("anonymous"))) {
			JSONObject loginFailed = new JSONObject();
			loginFailed.put("command", "LOGIN_FAILED");
			loginFailed.put("info", "user " + message.get("username") + " is not registered");
			sentmessage(loginFailed, con);
			return true;
		} else {
			if ((message.get("username").equals("anonymous")) ||
					(usersinfo.get(message.get("username")).equals(message.get("secret")))) {
				//LOGIN SUCCESS,REDIRECT

				try {
					String IP;
					int PORT ;
					int load = Integer.parseInt(
							allServerLoad.get(Server.getId()).get("load").toString());
					JSONObject redirectCommand = new JSONObject();
					for (JSONObject value : allServerLoad.values()) {
						if (Integer.parseInt(value.get("load").toString()) < (load - 1)) {
							IP = value.get("hostname").toString();
							PORT = Integer.parseInt(value.get("port").toString());
							redirectCommand.clear();
							redirectCommand.put("command", "REDIRECT");
							redirectCommand.put("hostname", IP);
							redirectCommand.put("port", PORT);
							sentmessage(redirectCommand, con);
//							redirect = false;
							log.info(message.get("username") + " has been redirected to: "
									+ IP + ":" + PORT);

							return true;
						}
					}

				} catch (NullPointerException e) {
					e.getMessage();
				}
				//=-------no direct
				JSONObject loginSuccess = new JSONObject();
				loginSuccess.put("command", "LOGIN_SUCCESS");
				loginSuccess.put("info", "logged in as user: " + username);
				sentmessage(loginSuccess, con);
				loginMap.put(con, message.get("username").toString());

//----------------------load change
				renewMyLoad();
				sendServerUpdate("SERVER_UPDATE", con);


//-------------------
				return false;


			} else {
				//LOGIN FAILED
				JSONObject loginFailed = new JSONObject();
				loginFailed.put("command", "LOGIN_FAILED");
				loginFailed.put("info", "wrong secret for user " + message.get("username"));
				sentmessage(loginFailed, con);
				return true;
			}
		}
	}

	public boolean register(JSONObject message, Connection con) throws IOException {
		if (message.get("secret") == null ||
				message.get("username") == null || message.size() != 3) {
			String ms = "incorrect message";
			return sendInvalidMessage(con, ms);
		}
//----------------Invalid message: already logged in on this connection
		if (loginMap.containsValue(con)) {
			loginMap.remove(con);
			String ms = "received " + message.get("command") + " from a client " +
					"that has already logged in as " + message.get("username");
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
//----------------------------------
			registerTime.put(message.get("username").toString(), System.currentTimeMillis());
//-------------------------------

			usersinfo.put(message.get("username"), message.get("secret"));
			broadcast(lockrequest, serverconnections);
			return false;
		}
	}

	public boolean lockRequest(String msg, JSONObject message, Connection con) throws IOException {
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
		return true;
	}

	public boolean lockDenied(String msg, JSONObject message, Connection con) throws IOException {
		if (message.get("secret") == null ||
				message.get("username") == null || message.size() != 3) {
			String ms = "incorrect message";
			return sendInvalidMessage(con, ms);
		}
		if (!serverconnections.contains(con)) {
			String ms = "received LOCK_DENIED from an unauthenticated server";
			return sendInvalidMessage(con, ms);
		}
		try {
			usersinfo.remove(message.get("username"));
		} catch (Exception e) {
			e.getMessage();
		}
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

	public boolean lockAllowed(String msg, JSONObject message, Connection con) throws IOException {
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
			if (i == allServerLoad.size()) {
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

	public boolean updateToOther(JSONObject message, Connection con) {
		String id = message.get("id").toString();
		try {
			JSONObject temp = new JSONObject();
			temp.put("load", message.get("load"));
			temp.put("port", message.get("port"));
			temp.put("ip", message.get("ip"));
//-----------自己存一份然后传给别人
			allServerLoad.put(id, temp);
			sendToOthers(con, message.toJSONString(), serverconnections);

		} catch (Exception e) {
			e.getMessage();
		}

		return false;
	}

	public boolean broadcastLoadRequest(JSONObject message, Connection con) throws IOException {
		message.put("command", "TARGET_SEND_LOAD");
		int num = allServerLoad.size()+1;


		int loadSum = 0;
		for (JSONObject load : allServerLoad.values()) {
			loadSum = loadSum + Integer.parseInt(load.get("load").toString());
		}
		try {
			int avg = loadSum / num;
//			log.warn("@@@@@@@@@@"+avg);
			for (Map.Entry<String, JSONObject> entry : allServerLoad.entrySet()) {
				int gap = Integer.parseInt(entry.getValue().get("load").toString()) - avg-1;
				if (gap > 0) {
					message.put(entry.getKey(), gap);

				}
			}

		} catch (NullPointerException e) {
			e.getMessage();
		}
//		log.warn("@@@@@@@@@@@@@@@@"+message);
		sentLoadOrNot(message, con);

		return false;
	}

	public void sentLoadOrNot(JSONObject message, Connection con) throws IOException {
		if (message.get(Server.getId()) != null) {
//			log.warn("!!!!!!!!!!!!!!!!"+message);
			int send = Integer.parseInt(message.get(Server.getId()).toString());
			int out = send;
			for (Connection key : loginMap.keySet()) {
				JSONObject redirect = new JSONObject();
				redirect.put("command", "REDIRECT");
				redirect.put("hostname", message.get("ip"));
				redirect.put("port", message.get("port"));
				log.warn("a user redirected to: " + message.get("ip") + "/" + message.get("port"));
				sentmessage(redirect, key);
				connectionClosed(key);

				send = send - 1;
				if (send == 0) {
					break;
				}
			}
			JSONObject announce2 = new JSONObject();
			announce2.put("command", "SERVER_UPDATE");
			announce2.put("id", Server.getId());
			announce2.put("load", connections.size() - serverconnections.size()-out);
			announce2.put("port", Settings.getLocalPort());
			announce2.put("ip", Settings.getIP());
			JSONObject temp = new JSONObject();
			temp.put("load", connections.size() - serverconnections.size()- out);


			temp.put("port", Settings.getLocalPort());
			temp.put("ip", Settings.getIP());
			String announcement = announce2.toJSONString();
			allServerLoad.put(Server.getId(),temp);
			broadcast(announcement, serverconnections);
		}

		sendToOthers(con, message.toJSONString(), serverconnections);
	}

	public boolean getreconnectInfo(JSONObject message) {

		reconnectUse = message;
		return false;
	}

	public boolean authenticate(JSONObject message, Connection con) throws IOException {
		if (message.get("secret") == null) {
			String ms = "the received message did not contain a secret";
			return sendInvalidMessage(con, ms);

		} else if (message.size() != 2) {
			String ms = "incorrect message";
			return sendInvalidMessage(con, ms);
		} else if (message.get("secret").equals(Settings.getSecret())) {

			sentmessage(usersinfo, con);
//-------------------------------------------------
//如果有父就传fatherInfo到reconnectInfo
			if (parent != null) {
				JSONObject p = new JSONObject();
				p.put("ip", parent.getSocket().getInetAddress().getHostAddress());
				p.put("port", connectionRemotePort.get(parent));
				reconnectInfo.put("father", p);
				if (nextRootNode != null && nextRootNode != con) {
					JSONObject renew = new JSONObject();
					renew.put("ROOT_RECONNECT", p);
					sentmessage(renew, nextRootNode);
				}
			}


//-----------------------------------------
			if ((parent != null && serverconnections.size() == 1) ||
					(parent == null && serverconnections.size() == 0)) {
				nextRootNode = con;
			}


			serverconnections.add(con);

			child.put(con, 0);
//---------------------------------
			if (reconnectInfo != null) {
//一定有父或子
				if (reconnectInfo.size() > 1) {
					sentmessage(reconnectInfo, con);
				} else {
//是根节点，而且没有子
					putreconnectInfo(con);
				}
			}
//--------------------------------------------------------
			return false;
		} else {
			JSONObject authenticateFail = new JSONObject();
			authenticateFail.put("command", "AUTHENTICATION_FAIL");
			authenticateFail.put("info", "the supplied secret is incorrect: " + message.get("secret"));
			sentmessage(authenticateFail, con);
			return true;
		}
	}

	public boolean reAuthenticate(JSONObject message, Connection con) throws IOException {
		boolean b2 = authenticate(message, con);
		if (!b2) {
			try {
				if (parent == null) {
//这里要添加Info信息	，已经拿到port了
					putreconnectInfo(con);
//如果这时候是根断了，重新连接到第二个根，发送空消息
					putreconnectInfo(con);
					JSONObject temp = new JSONObject();
					temp.put("command", "RECONNECT_INFO");
					sentmessage(temp, con);
				}
//一定有父或子
				else {
					sentmessage(reconnectInfo, con);
				}
			} catch (NullPointerException e) {
				e.getMessage();
			}

			JSONObject m = new JSONObject();
			m.put("command", "RECONNECT_SUCCESS");
			m.put("ip", con.getSocket().getLocalAddress().getHostAddress());
			m.put("port", con.getSocket().getLocalPort());
			sentmessage(m, con);
		}
		return b2;
	}

	public boolean reconnectSuccess(JSONObject message, Connection con) throws IOException {
		JSONObject m = new JSONObject();

		String ip = message.get("ip").toString();
		int port = Integer.parseInt(message.get("port").toString());
		m.put("ip", ip);
		m.put("port", port);
		reconnectInfo.clear();
		reconnectInfo.put("command", "RECONNECT_INFO");
		reconnectInfo.put("info", m);
		for (Connection c : serverconnections) {
			if (c != parent) {
				sentmessage(reconnectInfo, c);
			}
		}
		log.info("***** Reconnection success to : " + ip + ":  " + port);
		return false;
	}

	public boolean romotePort(JSONObject message, Connection con) throws IOException {
		JSONObject info = new JSONObject();
		info.put("port", message.get("port"));
		info.put("ip", con.getSocket().getInetAddress().getHostAddress());
		reconnectInfo.put("info", info);

		try {
			connectionRemotePort.put(con, Integer.parseInt(message.get("port").toString()));
		} catch (Exception e) {
			e.getMessage();
		}
		log.info("Get Connected with: " + con.getSocket().getInetAddress().getHostAddress() +
				"/ " + connectionRemotePort.get(con));
		return false;
	}


	/*
	 * The connection has been closed by the other party.
	 */
	public static synchronized void connectionClosed(Connection con) {
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

		//----------存自己的信息
		if (connections.size() == 1) {
			Settings.setIP(s.getLocalAddress().getHostAddress());
			JSONObject announce3 = new JSONObject();
			announce3.put("load", 0);
			announce3.put("port", Settings.getLocalPort());
			announce3.put("ip", s.getLocalAddress().getHostAddress());
			String id = Server.getId();
			allServerLoad.put(id, announce3);
		}
//----------------------

		return c;

	}

	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		Settings.setIP(s.getLocalAddress().getHostAddress());

		JSONObject outgo = new JSONObject();
		outgo.put("command", "AUTHENTICATE");
		outgo.put("secret", Settings.getSecret());
		sentmessage(outgo, c);

		connections.add(c);
		serverconnections.add(c);

		connectionRemotePort.put(c, Settings.getRemotePort());
		JSONObject p = new JSONObject();
		p.put("command", "REMOTE_PORT");
		p.put("port", Settings.getLocalPort());

//----------存自己的信息
		JSONObject announce3 = new JSONObject();
		announce3.put("load", 0);
		announce3.put("port", Settings.getLocalPort());
		announce3.put("ip", s.getLocalAddress().getHostAddress());
		String id = Server.getId();
		allServerLoad.put(id, announce3);
//----------------------请求load 进来
		JSONObject loadRequest = new JSONObject();
		loadRequest.put("command", "LOAD_REQUEST");
		loadRequest.put("ip", c.getSocket().getLocalAddress().getHostAddress());
		loadRequest.put("port", Settings.getLocalPort());
		sentmessage(loadRequest, c);

//------------------
		sentmessage(p, c);
		putreconnectInfo(c);


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
//		log.warn(allServerLoad + "##############");
//		log.warn("------------" + reconnectInfo);
//		log.warn("+++++++++++++++" + reconnectUse);


		if (child != null) {
			Iterator<Map.Entry<Connection, Integer>> it = child.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Connection, Integer> entry = it.next();
				Connection key = entry.getKey();
				Integer value = entry.getValue();

				if (value == 4) {
					connectionClosed(key);

					try {

						JSONObject temp = new JSONObject();
						temp = (JSONObject) reconnectInfo.get("info");
						String ip = temp.get("ip").toString();
						int port = Integer.parseInt(temp.get("port").toString());

//这个点是根，而且这个连接是最新的连接也就是reconnectinfo的信息。需要删除
						if (key.getSocket().getInetAddress().getHostAddress() == ip
								&& connectionRemotePort.get(key) == port) {
							if (key != nextRootNode) {
								temp.clear();
								temp.put("ip", nextRootNode.getSocket().getInetAddress().getHostAddress());
								temp.put("port", connectionRemotePort.get(nextRootNode));
								reconnectInfo.put("info", temp);
							} else {
								reconnectInfo.remove("info");
							}
						}
						it.remove();
						key.closeCon();
					} catch (Exception e) {
						e.getMessage();
					}

//					rootChangeReconnectInfo(key);

// 作为根节点， 或者reconnectInfo是断掉的server
				} else {
					int i = child.get(key) + 1;
					child.put(key, i);
				}
			}
		}

		if (parent != null) {
			triggerParent = judgeConnection(triggerParent, parent);
			if (triggerParent == 4) {
				parent.closeCon();
				connectionClosed(parent);
				try {
					if (reconnectUse == null || reconnectUse.size() == 1) {
						reconnection(rootReconnect);
					} else {
						reconnection(rootReconnect);
					}
				} catch (Exception e) {
					e.getMessage();
				}
			}


		}
//--------------------------------send SERVER_ANNOUNCE between severs
		sendServerAnnounce("SERVER_ANNOUNCE");
		return false;
	}

	public final void setTerm(boolean t) {
		term = t;
	}

	public static void sentmessage(JSONObject json, Connection con) throws IOException {
		Socket s = con.getSocket();
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(s.getOutputStream(),
						"UTF-8"));
		writer.write(json.toJSONString() + "\n");
		writer.flush();
	}

	public synchronized static void broadcast(String msg, ArrayList<Connection> connections) throws IOException {
		for (Connection con : connections) {
			Socket s = con.getSocket();
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(s.getOutputStream(),
							"UTF-8"));
			writer.write(msg + "\n");
			writer.flush();
		}
	}

	public int judgeConnection(int trigger, Connection con) {
		if (trigger == 3) {
			freeze = true;
			connections.remove(con);
			if (serverconnections.contains(con)) {
				serverconnections.remove(con);
			}
			con.closeCon();
		}
		return trigger + 1;
	}

	public static void sendToOthers(Connection con, String msg, ArrayList<Connection> connections) throws IOException {
		int index = connections.indexOf(con);
		ArrayList<Connection> temp = new ArrayList<Connection>();
		for (int x = 0; x < connections.size(); x++) {
			if (x != index) {
				temp.add(connections.get(x));
			}
		}
		broadcast(msg, temp);
	}


	public static void reconnection(JSONObject reconnectUse) throws IOException {
		JSONObject temp = new JSONObject();
		try {
			Socket s;

//两种情况，有父节点和根节点
			if (reconnectUse.get("father") != null) {
				temp = (JSONObject) reconnectUse.get("info");
				int port = Integer.parseInt(temp.get("port").toString());
				String ip = temp.get("ip").toString();
				s = new Socket(ip, port);
			} else {
				temp = (JSONObject) reconnectUse.get("info");
				int port = Integer.parseInt(temp.get("port").toString());
				String ip = temp.get("ip").toString();
				s = new Socket(ip, port);
			}


			log.debug("Reconnection begin！！");
			Connection c = new Connection(s);

			JSONObject outgo = new JSONObject();
			outgo.put("command", "RE_AUTHENTICATE");
			outgo.put("secret", Settings.getSecret());

			sentmessage(outgo, c);

			JSONObject p = new JSONObject();
			p.put("command", "REMOTE_PORT");
			p.put("port", Settings.getLocalPort());
			sentmessage(p, c);

			connections.add(c);
			serverconnections.add(c);
			reconnectInfo.clear();
			reconnectInfo.put("command", "RECONNECT_INFO");
			reconnectInfo.put("info", temp);

		} catch (Exception e) {
			e.getMessage();
		}

	}

	public JSONObject connectionToJson(Connection con) {
		String ip = con.getSocket().getInetAddress().getHostAddress();
		JSONObject info = new JSONObject();
		info.put("ip", ip);
		info.put("port", connectionRemotePort.get(con));
		return info;
	}

	public synchronized void putreconnectInfo(Connection con) {
		JSONObject temp = new JSONObject();
		temp = connectionToJson(con);
		reconnectInfo.clear();
		reconnectInfo.put("command", "RECONNECT_INFO");
		reconnectInfo.put("info", temp);

	}

	public void sendServerUpdate(String msg, Connection con) throws IOException {
		JSONObject announce2 = new JSONObject();
		announce2.put("command", msg);
		announce2.put("id", Server.getId());
		announce2.put("load", connections.size() - serverconnections.size());
		announce2.put("port", Settings.getLocalPort());
		announce2.put("ip", con.getSocket().getLocalAddress().getHostAddress());
		String announcement = announce2.toJSONString();
		broadcast(announcement, serverconnections);
	}

	public void sendServerAnnounce(String msg) {
		try {

			JSONObject announce = new JSONObject();
			announce.put("command", msg);
			announce.put("id", Server.getId());
			announce.put("load", connections.size() - serverconnections.size());
			announce.put("hostname", Settings.getLocalHostname());
			announce.put("port", Settings.getLocalPort());
			String announcement = announce.toJSONString();
			broadcast(announcement, serverconnections);

		} catch (Exception e) {
			e.getMessage();
		}
	}

	public static void childReconnection() {

		try {
			if (reconnectUse != null && reconnectUse.size() > 1) {
				reconnection(reconnectUse);
			} else {
// 说明是根
				JSONObject obj = new JSONObject();
				obj.put("command", "RECONNECT_INFO");
				setReconnectInfo(obj);
				sentmessage(obj, nextRootNode);
				JSONObject temp = new JSONObject();
//除了nextRootNode 删除父父节点，用同级重连接
				temp.put("command", "DELETE_FATHER");
				sendToOthers(nextRootNode, temp.toJSONString(), serverconnections);
			}

		} catch (Exception z) {
			log.error("reconnection: " + z.getMessage());
		}
	}

	public static void parentDo(Connection con) {
		//作为父
//		else if (this == Control.nextRootNode) {
//
//		}
		Iterator<Map.Entry<Connection, Integer>> it = child.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Connection, Integer> entry = it.next();
			Connection key = entry.getKey();
			if (con == key) {
				it.remove();
			}
		}
	}

	public static void registerTimeout() throws IOException {
		if (registerTime.size() != 0) {
			long now = System.currentTimeMillis();
			Iterator<Map.Entry<String, Long>> it = registerTime.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Long> entry = it.next();
				String key = entry.getKey();
				Long t = entry.getValue();
				long gap = now - t;
				if (gap < 6000) {
					//----------------------Lock_deny
					JSONObject lock_denied = new JSONObject();
					lock_denied.put("command", "LOCK_DENIED");
					lock_denied.put("username", key);
					lock_denied.put("secret", "default");
					String lockdeny = lock_denied.toJSONString();
					broadcast(lockdeny, serverconnections);
					usersinfo.remove(key);
//-------------------------regster_failed
					JSONObject registerFail = new JSONObject();
					registerFail.put("command", "REGISTER_FAILED");
					registerFail.put("info", "Register time out, please try later!!");
					sentmessage(registerFail, connectionMap.get(key));
//===========clear map
					connectionMap.remove(key);
					registerTime.remove(key);
					it.remove();
				}
			}
		}
	}

	//-------------从server announce中提取出的redirect
	public void redirection(JSONObject message) {

		boolean flag = true;

		Iterator<Map.Entry<String, JSONObject>> it = allServerLoad.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, JSONObject> entry = it.next();
			String key = entry.getKey();
			JSONObject i = entry.getValue();

			if (key.equals(message.get("id"))) {
				i.put("load", message.get("load"));
				flag = false;
				break;
			}
		}
		try {
			if (flag) {
				JSONObject newServer = new JSONObject();
				newServer.put("load", message.get("load"));
				newServer.put("hostname", message.get("hostname"));
				newServer.put("port", message.get("port"));
				allServerLoad.put(message.get("id").toString(), newServer);
			}

		} catch (Exception e) {
			e.getMessage();
		}



	}

	public void renewMyLoad() {
		JSONObject announce3 = new JSONObject();
		announce3.put("load", connections.size() - serverconnections.size());
		announce3.put("ip",Settings.getIP());
		announce3.put("port", Settings.getLocalPort());
		allServerLoad.put(Server.getId(), announce3);
	}

}
