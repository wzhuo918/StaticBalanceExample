import java.net.*;
import java.io.*;
import java.lang.*;

public class TaskTrackerServerSocket extends Thread {
	/*
	 * public ServerSocket server; public Socket client;
	 * 
	 * public ServerSocket getServer() { return server; } public void
	 * setServer(ServerSocket server) { this.server = server; } public Socket
	 * getClient() { return client; } public void setClient(Socket client) {
	 * this.client = client; }
	 */

	public TaskTrackerServerSocket() {
	}

	public void ServerServiceUp(ServerSocket server, Socket client, String data)
			throws Exception {
		// single map(multiple maps needed to be improved)
		client = server.accept();
		long result;

		BufferedReader in = new BufferedReader(new InputStreamReader(
				client.getInputStream()));
		PrintWriter out = new PrintWriter(client.getOutputStream());

		while (true) {
			out.println(data);
			out.flush();
			break;
		}
		in.close();
		out.close();
	}

	public void run(String data) {
		ServerSocket server_tmp = null;
		Socket client = null;
		int port = 5678;
		int flag = 0;
		for (int i = 0; i < 10 && flag == 0; i++) {
			port = 5678;
			while (port <= 5688) {
				try {
					server_tmp = new ServerSocket(port);
					flag = 1;
					// ServerServiceUp(data);
					break;
				} catch (IOException e) {
					port++;
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}// while
		}

		if (flag == 1) {
			try {
				ServerServiceUp(server_tmp, client, data);
				server_tmp.close();
				if (client != null) {
					client.close();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}