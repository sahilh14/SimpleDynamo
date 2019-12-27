package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.net.InetSocketAddress;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static ArrayList<String> ring = new ArrayList<String>();
	static HashMap<String, String> port_hash_map = new HashMap<String, String>();
	static HashMap<String, Integer> port_socket_map = new HashMap<String, Integer>();
	static String successor1;
	static String successor2;
	static String predecessor1;
	static String predecessor2;
	static String encoded_port;
	static boolean query_request = false;
	static String[] ports = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
	static Socket[] s_sockets = new Socket[4];
	static ObjectOutputStream[] s_out = new ObjectOutputStream[4];
	static DataInputStream[] s_in = new DataInputStream[4];
	static ReentrantLock deletelock = new ReentrantLock();
	static Semaphore semaphore = new Semaphore(3);
	static ReentrantLock filelock = new ReentrantLock();
	static Semaphore fileaccess = new Semaphore(3);
	static Semaphore oncreate = new Semaphore(2);

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Context ctx = getContext();
		Log.v("delete", selection);
		ArrayList<String> files = new ArrayList<String>();
		String node_type = "";
		int index;

		if (selection.equals("@") || selection.equals("*")){
			files.addAll(Arrays.asList(ctx.fileList()));
			if(selection.equals("*") && selectionArgs == null) {
				for (ObjectOutputStream out : s_out){
					try{
						out.writeUTF("deletion");
						out.flush();
						out.writeUTF(selection);
						out.flush();
					} catch (IOException e) {
						Log.e(TAG, "IOException" + e.getMessage());
					}
				}
			}
		} else {
			files.add(selection);
			try{
				if (selectionArgs == null){
					int correct_node = -1;
					String val = genHash(selection);
					String p = genHash(String.valueOf((Integer.parseInt(port_hash_map.get(predecessor1)) / 2)));
					if ((val.compareTo(p) > 0 && val.compareTo(encoded_port) <= 0) ||
							(val.compareTo(p) > 0 && encoded_port.compareTo(p) < 0) ||
							(val.compareTo(p) < 0 && val.compareTo(encoded_port) <= 0 && encoded_port.compareTo(p) < 0)){
						node_type = "coordinator";
					} else {
						for ( int i = 0; i <= 4; i++){
							if (val.compareTo(ring.get(i)) < 0) {
								if (i == 0) {
									//this node has it
									correct_node = i;
									break;
								} else if (val.compareTo(ring.get(i - 1)) > 0) {
									//this node has it
									correct_node = i;
									break;
								}
							}
							else if(i == 4 && val.compareTo(ring.get(i)) > 0){
								// 0th node has it
								correct_node = 0;
								break;
							}
						}
						try{
							deletelock.lock();
							index = port_socket_map.get(port_hash_map.get(ring.get(correct_node)));
							try {
								fwdDelete(index, selection);
							} catch (IOException e) {
								Log.e(TAG, "IOException while deleting from coordinator" + e.getMessage());
							} catch (NullPointerException e) {
								Log.e(TAG, "NullPointerException while deleting from coordinator" + e.getMessage());
							}
							int succ1 = (correct_node + 5 + 1) % 5;
							if (!encoded_port.equals(ring.get(succ1))) {
								try {
									index = port_socket_map.get(port_hash_map.get(ring.get(succ1)));
									fwdDelete(index, selection);
								} catch (IOException e) {
									Log.e(TAG, "IOException while deleting from successor1" + e.getMessage());
								} catch (NullPointerException e) {
									Log.e(TAG, "NullPointerException while deleting from successor1" + e.getMessage());
								}
							} else {
								node_type = "successor1";
							}
							int succ2 = (correct_node + 5 + 2) % 5;
							if (!encoded_port.equals(ring.get(succ2))) {
								try {
									index = port_socket_map.get(port_hash_map.get(ring.get(succ2)));
									fwdDelete(index, selection);
								} catch (IOException e) {
									Log.e(TAG, "IOException while deleting from successor2" + e.getMessage());
								} catch (NullPointerException e) {
									Log.e(TAG, "NullPointerException while deleting from successor2" + e.getMessage());
								}
							} else {
								node_type = "successor2";
							}
						} finally {
							deletelock.unlock();
						}
						if (!node_type.equals("successor1") && !node_type.equals("successor2"))
							return 0;
					}
				}
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "NoSuchAlgorithmException" + e.getMessage());
			}
		}
		try {
			filelock.lock();
			for (String filename: files) {
				ctx.deleteFile(filename);
			}
		} catch (Exception e) {
			Log.e(TAG, "CP - Delete failed");
		} finally {
			filelock.unlock();
		}
		if (node_type.equals("coordinator")) {
			deletelock.lock();
			try {
				try {
					index = port_socket_map.get(port_hash_map.get(successor1));
					fwdDelete(index, selection);
				} catch (IOException e) {
					Log.e(TAG, "IOException in calling deletion1" + e.getMessage());
				} catch (NullPointerException e) {
					Log.e(TAG, "NullPointerException in calling deletion1" + e.getMessage());
				}
				try{
					index = port_socket_map.get(port_hash_map.get(successor2));
					fwdDelete(index, selection);
				} catch (IOException e) {
					Log.e(TAG, "IOException in calling deletion2" + e.getMessage());
				} catch (NullPointerException e) {
					Log.e(TAG, "NullPointerException in calling deletion2" + e.getMessage());
				}
			} finally {
				deletelock.unlock();
			}
		}
		return 0;
	}

	static void fwdDelete(int index, String filename) throws IOException, NullPointerException {
		try {
			s_out[index].writeUTF("deletion");
			s_out[index].flush();
			s_out[index].writeUTF(filename);
			s_out[index].flush();
		} catch (IOException e) {
			throw e;
		} catch (NullPointerException e) {
			throw e;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		try {
			oncreate.acquire(1);
			Log.v(TAG, "Acquired 1 oncreate in insert");
		} catch (InterruptedException e) {
			Log.e(TAG, e.getMessage());
		} finally {
			oncreate.release(1);
		}

		int index;
		Context ctx = getContext();
		String filename = values.getAsString("key");
		String data = values.getAsString("value");
		String type = values.getAsString("type");
		if (type == null){
			try{
				int correct_node = -1;
				type = "insertion";
				String val = genHash(filename);
				String p = genHash(String.valueOf((Integer.parseInt(port_hash_map.get(predecessor1)) / 2)));

				if ((val.compareTo(p) > 0 && val.compareTo(encoded_port) <= 0) ||
						(val.compareTo(p) > 0 && encoded_port.compareTo(p) < 0) ||
						(val.compareTo(p) < 0 && val.compareTo(encoded_port) <= 0 && encoded_port.compareTo(p) < 0)){
					type = "nativeinsertion";

				} else{
					for ( int i = 0; i <= 4; i++){
						if (val.compareTo(ring.get(i)) < 0) {
							if (i == 0) {
								correct_node = i;
								break;
							} else if (val.compareTo(ring.get(i - 1)) > 0) {
								correct_node = i;
								break;
							}
						}
						else if(i == 4 && val.compareTo(ring.get(i)) > 0){
							correct_node = 0;
							break;
						}
					}
					try{
						try {
							semaphore.acquire(3);
						} catch (InterruptedException e) {
							Log.e(TAG, e.getMessage());
						}
						Log.v("insert", values.toString());
						Log.v(TAG, "Target node for insertion" + port_hash_map.get(ring.get(correct_node)));
						index = port_socket_map.get(port_hash_map.get(ring.get(correct_node)));
						try {
							fwdInsert(index, filename, data);
						} catch (IOException e) {
							Log.e(TAG, "IOException while inserting to remote node" +  e.getMessage());
						} catch (NullPointerException e){
							Log.e(TAG, "NullPointerException while inserting to remote node" + e.getMessage());
						}

						int succ1 = (correct_node + 5 + 1) % 5;
						Log.v(TAG, "Target node for insertion" +  port_hash_map.get(ring.get(succ1)));
						if (!encoded_port.equals(ring.get(succ1))) {
							index = port_socket_map.get(port_hash_map.get(ring.get(succ1)));
							try {
								fwdInsert(index, filename, data);
							} catch (IOException e) {
								Log.e(TAG, "IOException while inserting to remote node + 1" +  e.getMessage());
							} catch (NullPointerException e){
								Log.e(TAG, "NullPointerException while inserting to remote node + 1" + e.getMessage());
							}
						} else {
							type = "replication1";
						}

						int succ2 = (correct_node + 5 + 2) % 5;
						Log.v(TAG, "Target node for insertion" +  port_hash_map.get(ring.get(succ2)));
						if (!encoded_port.equals(ring.get(succ2))) {
							index = port_socket_map.get(port_hash_map.get(ring.get(succ2)));
							try {
								fwdInsert(index, filename, data);
							} catch (IOException e) {
								Log.e(TAG, "IOException while inserting to remote node + 2" +  e.getMessage());
							} catch (NullPointerException e){
								Log.e(TAG, "NullPointerException while inserting to remote node + 2" + e.getMessage());
							}
						} else {
							type = "replication2";
						}
					} finally {
						semaphore.release(3);
					}
					if (type.equals("insertion")) {
						return null;
					}
				}
			} catch (NoSuchAlgorithmException e){
				Log.e(TAG, "genHash exception" + e.getMessage());
			}
		}
		String string;
		if (type.equals("recovery")) {
			string = data;
		} else {
			string = data + "\n";
		}
		Log.v("insert", values.toString());
		FileOutputStream outputStream;
		if (!type.equals("recovery")) {
			try {
				fileaccess.acquire(3);
			} catch (InterruptedException e){
				Log.e(TAG, e.getMessage());
			} finally {
				fileaccess.release(3);
			}
		}
		try {
			filelock.lock();
			outputStream = ctx.openFileOutput(filename, ctx.MODE_PRIVATE);
			outputStream.write(string.getBytes());
			outputStream.close();
		} catch (Exception e) {
			Log.e(TAG, "CP - File write failed - " + filename);
		} finally {
			filelock.unlock();
		}
		if (type.equals("nativeinsertion")){
			try{
				try {
					semaphore.acquire(3);
				} catch (InterruptedException e) {
					Log.e(TAG, e.getMessage());
				}
				index = port_socket_map.get(port_hash_map.get(successor1));
				try {
					fwdInsert(index, filename, data);
				} catch (IOException e) {
					Log.e(TAG, "replication 1 IOException - " + port_hash_map.get(successor1) + e.getMessage());
				} catch (NullPointerException e){
					Log.e(TAG, "replication 1 NullPointerException - " + port_hash_map.get(successor1) + e.getMessage());
				}
				index = port_socket_map.get(port_hash_map.get(successor2));
				try {
					fwdInsert(index, filename, data);
				} catch (IOException e) {
					Log.e(TAG, "replication 2 IOException - " + port_hash_map.get(successor2) + e.getMessage());
				} catch (NullPointerException e){
					Log.e(TAG, "replication 2 NullPointerException - " + port_hash_map.get(successor2) + e.getMessage());
				}
			} finally {
				semaphore.release(3);
			}
		}

		return Uri.withAppendedPath(uri, filename);
	}

	static void fwdInsert(int index, String filename, String data) throws IOException, NullPointerException {
		try {
			s_out[index].writeUTF("insertion");
			s_out[index].flush();
			s_out[index].writeUTF(filename);
			s_out[index].writeUTF(data);
			s_out[index].flush();
		} catch (IOException e) {
			throw e;
		} catch (NullPointerException e) {
			throw e;
		}
	}

	@Override
	public boolean onCreate() {
		try {
			oncreate.acquire(2);
			Log.v(TAG, "Acquired 2 oncreate in oncreate");
		} catch (InterruptedException e) {
			Log.e(TAG, e.getMessage());
		}
		Context ctx = getContext();
		TelephonyManager tel = (TelephonyManager) ctx.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try{
			encoded_port = genHash(portStr);

		} catch (NoSuchAlgorithmException e){
			Log.e(TAG, "genHash exception" + e.getMessage());
		}
		ring.add(encoded_port);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
		return false;
	}


	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {
				fileaccess.acquire(3);
				Log.v(TAG, "Acquired 3 fileaccess in oncreate");
			} catch (InterruptedException e) {
				Log.e(TAG, e.getMessage());
			}

			String val;
			Context ctx = getContext();
			int i = 0;
			for (String s : ports) {
				if (!msgs[0].equals(s)) {
					s_sockets[i] = new Socket();
					try {
						val = genHash(String.valueOf((Integer.parseInt(s) / 2)));

					} catch (NoSuchAlgorithmException e) {
						val = "";
						Log.e(TAG, "ClientTask NoSuchAlgorithmException" + e.getMessage());
					}
					try {
						s_sockets[i].connect(new InetSocketAddress(
								InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s)), 800);
					} catch (Exception e) {
						Log.e(TAG, "ClientTask socket creation failure" + e.getMessage());
						s_in[i] = null;
						s_out[i] = null;
						port_socket_map.put(s, i);
						port_hash_map.put(val, s);
						ring.add(val);
						i++;
						continue;
					}
					port_socket_map.put(s, i);
					port_hash_map.put(val, s);
					ring.add(val);
					try {
						s_in[i] = new DataInputStream(s_sockets[i].getInputStream());
						s_out[i] = new ObjectOutputStream(s_sockets[i].getOutputStream());
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket failure" + e.getMessage());
						try {
						    try {
						        Thread.sleep(100);
                            } catch (InterruptedException e1) {
                                Log.e(TAG, e1.getMessage());
                            }
                            s_sockets[i].close();
							s_sockets[i].connect(new InetSocketAddress(
									InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s)), 500);
							s_in[i] = new DataInputStream(s_sockets[i].getInputStream());
							s_out[i] = new ObjectOutputStream(s_sockets[i].getOutputStream());
						} catch (Exception e1) {
							Log.e(TAG, "ClientTask socket creation failure" + e1.getMessage());
							s_in[i] = null;
							s_out[i] = null;
						}
					}
					i++;
				}
			}
			Thread[] threads = new Thread[4];
            for (i=0; i<4; i++) {
                Runnable r = new Arrival(s_out[i], s_in[i], msgs[0]);
                threads[i] = new Thread(r);
                threads[i].start();
            }
//			for (ObjectOutputStream out : s_out) {
//				Runnable r = new Arrival(out , msgs[0]);
//				threads[i] = new Thread(r);
//				threads[i].start();
//				i++;
//			}
//			String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
//			Uri providerURI = Uri.parse("content://" + authority);
//			try {
//				ctx.getContentResolver().delete(providerURI, "@", null);
//				Log.v(TAG, "Files after deletion" + (ctx.fileList()).length);
//			} catch (NullPointerException e) {
//				Log.e(TAG, "NullPointerException clientTask delete failure - " + e.getMessage());
//			}

			Collections.sort(ring);
			int myindex = ring.indexOf(encoded_port);
			successor1 = ring.get((myindex + 5 + 1) % 5);
			successor2 = ring.get((myindex + 5 + 2) % 5);
			predecessor1 = ring.get((myindex + 5 - 1) % 5);
			predecessor2 = ring.get((myindex + 5 - 2) % 5);
			oncreate.release(2);
			for (i = 0; i < 4; i++){
				try{
					threads[i].join();
				} catch (InterruptedException e){
					Log.e(TAG, "Threads done");
					Log.v(TAG, "Threads done");
				}
			}
			System.out.println("all Threads done");
			Runnable r1 = new Recovery("first");
			Runnable r2 = new Recovery("second");
			Runnable r3 = new Recovery("third");
			new Thread(r1).start();
			new Thread(r2).start();
			new Thread(r3).start();
			return null;
		}
	}

	public class Arrival implements Runnable {
		private ObjectOutputStream out;
		private DataInputStream in;
		private String port;

		Arrival(ObjectOutputStream out, DataInputStream in, String port) {
			this.out = out;
            this.in = in;
			this.port = port;
		}
		public void run(){
			try {
				out.writeUTF("join request");
				out.flush();
				out.writeUTF(port);
				out.flush();
				String ack = in.readUTF();
			} catch (IOException e) {
				Log.e(TAG, "ClientTask join request failure - " + e.getMessage());
			} catch (NullPointerException e) {
				Log.e(TAG, "NullPointerException clientTask join request failure - " + e.getMessage());
			}
		}

	}

	public class Recovery implements Runnable {
		private String recover_from;
		private String recover_for;

		Recovery(String type){
			if (type.equals("first")) {
				this.recover_from = successor1;
				this.recover_for = predecessor1;
			}
			if (type.equals("second")) {
				this.recover_from = predecessor1;
				this.recover_for = predecessor2;
			}
			if (type.equals("third")) {
				this.recover_from = successor2;
				this.recover_for = encoded_port;
			}
		}

		public void run() {
			try {
				ArrayList<ContentValues> values = new ArrayList<ContentValues>();
				String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
				Uri providerURI = Uri.parse("content://" + authority);

				try {
					semaphore.acquire(1);
				} catch (InterruptedException e) {
					Log.e(TAG, e.getMessage());
				}
				s_out[port_socket_map.get(port_hash_map.get(recover_from))].writeUTF("recovery");
				s_out[port_socket_map.get(port_hash_map.get(recover_from))].flush();
				s_out[port_socket_map.get(port_hash_map.get(recover_from))].writeUTF(recover_for);
				s_out[port_socket_map.get(port_hash_map.get(recover_from))].flush();
				String query_return = s_in[port_socket_map.get(port_hash_map.get(recover_from))].readUTF();
				if (query_return.equals("Initiating recovery response")){
					int count = s_in[port_socket_map.get(port_hash_map.get(recover_from))].readInt();
					Log.v(TAG, "Count of files in recovery" + count);
					Log.v(TAG, "Source" + port_hash_map.get(recover_from));
					Log.v(TAG, "Target" + port_hash_map.get(recover_for));

					for(int j = 0; j < count; j++){
						String key = s_in[port_socket_map.get(port_hash_map.get(recover_from))].readUTF();
						String data = s_in[port_socket_map.get(port_hash_map.get(recover_from))].readUTF();
						ContentValues value = new ContentValues();
						value.put("key", key);
						value.put("value", data);
						value.put("type", "recovery");
						values.add(value);

					}
					ContentValues[] v = new ContentValues[values.size()];
					v = values.toArray(v);
					bulkInsert(providerURI, v);
				}
			} catch (IOException e) {
				Log.e(TAG, "ClientTask IOException while recovery" + e.getMessage());
			} catch (NullPointerException e) {
                Log.e(TAG, "ClientTask NullPointerException while recovery" + e.getMessage());
            } finally {
				Log.v(TAG, "Recovery done");
				semaphore.release(1);
				fileaccess.release(1);
				Log.v(TAG, "Released 1 fileaccess in onCreate");
			}
		}
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			ExecutorService threadPool = Executors.newFixedThreadPool(10);
			try {
				while (true) {
					threadPool.execute(new ClientHandler(serverSocket.accept()));
				}
			} catch (Exception e) {
				Log.e(TAG, "ServerTask Connection acceptance failure" + e.getMessage());
			}
			return null;
		}


		private class ClientHandler implements Runnable {
			private Socket socket;
			private Context ctx;

			ClientHandler(Socket socket) {
				this.socket = socket;
				this.ctx = getContext();
			}

			@Override
			public void run() {
				String input;
				String port = null;
				ObjectInputStream in;
				DataOutputStream out;
				try {
					in = new ObjectInputStream(socket.getInputStream());
					out = new DataOutputStream(socket.getOutputStream());
					while (true) {
						input = in.readUTF();
						if (input.equals("join request")){
							try {
								semaphore.acquire(1);
							} catch (InterruptedException e) {
								Log.e(TAG, e.getMessage());
							}
							port = in.readUTF();
							Log.v(TAG, "Connected to client" + port);
							int j = port_socket_map.get(port);
							try {
								try {
									s_sockets[j].close();
								} catch (IOException e) {
									Log.e(TAG, "socket closing failure" + e.getMessage());
								}
								s_sockets[j] = new Socket();
								s_sockets[j].connect(new InetSocketAddress(
										InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port)), 2000);
								s_in[j] = new DataInputStream(s_sockets[j].getInputStream());
								s_out[j] = new ObjectOutputStream(s_sockets[j].getOutputStream());
								s_out[j].writeUTF("acknowledgement");
								s_out[j].flush();
								s_out[j].writeUTF(encoded_port);
								s_out[j].flush();
								out.writeUTF("A");
								out.flush();
							} catch (IOException e) {
								Log.e(TAG, "ClientTask socket creation failure" + e.getMessage());
							} finally {
								semaphore.release(1);
							}
						}
						if (input.equals("acknowledgement")) {
							port = in.readUTF();
							port = port_hash_map.get(port);
							Log.v(TAG, "Connected to client" + port);
						}
						if (input.equals("recovery")){
							String hashed_val = in.readUTF();
							String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
							Uri providerURI = Uri.parse("content://" + authority);
							String[] selection = new String[1];
							selection[0] = hashed_val;
							Cursor c = ctx.getContentResolver().query(providerURI, null, null, selection, null);
							if (c != null){
								c.moveToFirst();
								out.writeUTF("Initiating recovery response");
								out.flush();
								int size = c.getCount();
								out.writeInt(size);
								out.flush();
								for(int i=0; i < size; i++){
									out.writeUTF(c.getString(0));
									out.writeUTF(c.getString(1));
									out.flush();
									c.moveToNext();
								}
								c.close();
							} else {
								out.writeUTF("Initiating recovery response");
								out.writeInt(0);
								out.flush();
							}
						}
						if (input.equals("insertion")){
							String key = in.readUTF();
							String data = in.readUTF();
							String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
							Uri providerURI = Uri.parse("content://" + authority);
							ContentValues values = new ContentValues();
							values.put("key", key);
							values.put("value", data);
							values.put("type", "insertion");
							ctx.getContentResolver().insert(providerURI, values);
						}
						if (input.equals("query")){
							String key = in.readUTF();
							String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
							Uri providerURI = Uri.parse("content://" + authority);
							Cursor c = ctx.getContentResolver().query(providerURI, null, key, null, null);
							if (c != null){
								c.moveToFirst();
								out.writeUTF("SQR");
								out.flush();
								int size = c.getCount();
								out.writeInt(size);
								out.flush();
								for(int i=0; i < size; i++){
									out.writeUTF(c.getString(0));
									out.writeUTF(c.getString(1));
									out.flush();
									c.moveToNext();
								}
								c.close();
							} else {
								out.writeUTF("SQR");
								out.writeInt(0);
								out.flush();
							}
						}
						if (input.equals("1query")){
							String key = in.readUTF();
							String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
							Uri providerURI = Uri.parse("content://" + authority);
							String[] selection = new String[1];
							selection[0] = "1query";
							Cursor c = ctx.getContentResolver().query(providerURI, null, key, selection, null);
							if (c != null){
								c.moveToFirst();
								out.writeUTF("SQR");
								out.flush();
								int size = c.getCount();
								out.writeInt(size);
								out.flush();
								for(int i=0; i < size; i++){
									out.writeUTF(c.getString(0));
									out.writeUTF(c.getString(1));
									out.flush();
									c.moveToNext();
								}
								c.close();
							} else {
								out.writeUTF("SQR");
								out.writeInt(0);
								out.flush();
							}
						}

						if (input.equals("deletion")){
							String key = in.readUTF();
							String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
							Uri providerURI = Uri.parse("content://" + authority);
							String[] selection = new String[1];
							selection[0] = "deletion";
							ctx.getContentResolver().delete(providerURI, key, selection);
						}
					}
				} catch (IOException e) {
					Log.e(TAG, "Stream error" + e.getMessage());
					int j;
					if (port != null) {
						j = port_socket_map.get(port);
						try {
							Log.v(TAG, "Closing socket" + port);
							s_sockets[j].close();
							s_out[j] = null;
							s_in[j] = null;
						} catch (IOException e1) {
							Log.e(TAG, "Stream error while closing the socket" + e1.getMessage());
						}
					}
				}
			}
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		try {
			oncreate.acquire(1);
			Log.v(TAG, "Acquired 1 oncreate in query");
		} catch (InterruptedException e) {
			Log.e(TAG, e.getMessage());
		} finally {
			oncreate.release(1);
		}

		Context ctx = getContext();
		ArrayList<String> files = new ArrayList<String>();
		MatrixCursor cursor = new MatrixCursor( new String[] {"key", "value"});

		if (selectionArgs != null && !selectionArgs[0].equals("1query")) {
			int recovernode_index = ring.indexOf(selectionArgs[0]);
			int predofnode_index = (recovernode_index + 5 - 1) % 5;

			files.addAll(Arrays.asList(ctx.fileList()));
			FileInputStream inputStream;
			String val = "";
			for (String filename: files) {
				try {
					val = genHash(filename);
				} catch (NoSuchAlgorithmException e){
					Log.e(TAG,"NoSuchAlgorithmException" + e.getMessage());
				}
				if ((val.compareTo(ring.get(predofnode_index)) > 0 && val.compareTo(selectionArgs[0]) <= 0) ||
						(val.compareTo(ring.get(predofnode_index)) > 0 && selectionArgs[0].compareTo(ring.get(predofnode_index)) < 0) ||
						(val.compareTo(ring.get(predofnode_index)) < 0 && val.compareTo(selectionArgs[0]) <= 0 && selectionArgs[0].compareTo(ring.get(predofnode_index)) < 0)) {
					StringBuilder string = new StringBuilder();
					try {
						filelock.lock();
						inputStream = ctx.openFileInput(filename);
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						String l;
						while ((l = bufferedReader.readLine()) != null) {
							string.append(l);
						}
						inputStreamReader.close();
						inputStream.close();
					} catch (Exception e) {
						Log.e(TAG, "CP - File read failed - " + filename);
					} finally {
						filelock.unlock();
					}

					try {
						cursor.newRow()
								.add("key", filename)
								.add("value", string);

					} catch (Exception ex) {
						Log.e(TAG, "Matrix cursor failed");
					}
				}
			}
			cursor.close();
			return cursor;
		}
		if (query_request){
			query_request = false;
			return null;
		}

		int index;
		Log.v("query", selection);
		if (selection.equals("@") || selection.equals("*")){
			query_request = true;
//			files.addAll(Arrays.asList(ctx.fileList()));
			if(selection.equals("*") && successor1 != null) {
				try{
					index = port_socket_map.get(port_hash_map.get(successor1));
					cursor = fwdQuery(index, selection, "query");
				} catch (IOException e) {
					Log.e(TAG, "IOException in * while contacting successor 1 -" + successor1 + e.getMessage());
					try{
						index = port_socket_map.get(port_hash_map.get(successor2));
						cursor = fwdQuery(index, selection, "query");
					} catch (IOException e1) {
						Log.e(TAG, "IOException in * while contacting successor 2 -" + successor2 + e1.getMessage());
					} catch (NullPointerException e1) {
						Log.e(TAG, "NullPointerException in * while contacting successor 2 -" + successor2 + e1.getMessage());
					}
				} catch (NullPointerException e) {
					Log.e(TAG, "NullPointerException in * while contacting successor 1 -" + successor1 + e.getMessage());
					try{
						index = port_socket_map.get(port_hash_map.get(successor2));
						cursor = fwdQuery(index, selection, "query");
					} catch (IOException e1) {
						Log.e(TAG, "IOException in * while contacting successor 2 with value -" + successor2 + e1.getMessage());
					} catch (NullPointerException e1) {
						Log.e(TAG, "NullPointerException in * while contacting successor 2 -" + successor2 + e1.getMessage());
					}
				}
			}
		} else if(selectionArgs == null) {
			try{
				int j = 0;
				String val = genHash(selection);
				String p = genHash(String.valueOf((Integer.parseInt(port_hash_map.get(predecessor1)) / 2)));
				if ((val.compareTo(p) > 0 && val.compareTo(encoded_port) <= 0) ||
						(val.compareTo(p) > 0 && encoded_port.compareTo(p) < 0) ||
						(val.compareTo(p) < 0 && val.compareTo(encoded_port) <= 0 && encoded_port.compareTo(p) < 0)) {
					try {
						try {
							semaphore.acquire(3);
						} catch (InterruptedException e) {
							Log.e(TAG, e.getMessage());
						}
						Log.v(TAG, "Port with data" + port_hash_map.get(successor2));
						index = port_socket_map.get(port_hash_map.get(successor2));
						cursor = fwdQuery(index, selection, "1query");
						return cursor;
					} catch (IOException e) {
						Log.e(TAG, "1 query IOException while reading from successor 2 " + e.getMessage());
						try {
							Log.v(TAG, "Port off" + port_hash_map.get(successor2));
							Log.v(TAG, "Port with data" + port_hash_map.get(successor1));
							index = port_socket_map.get(port_hash_map.get(successor1));
							cursor = fwdQuery(index, selection, "1query");
							return cursor;
						} catch (IOException e1) {
							Log.e(TAG, "1 query IOException while reading from successor 1" + e1.getMessage());
						} catch (NullPointerException e1) {
							Log.e(TAG, "1 query NullPointerException while reading from successor 1" + e1.getMessage());
						}
					} catch (NullPointerException e) {
						Log.e(TAG, "1 query NullPointerException while reading from successor 2 " + e.getMessage());
						try {
							Log.v(TAG, "Port off" + port_hash_map.get(successor2));
							Log.v(TAG, "Port with data" + port_hash_map.get(successor1));
							index = port_socket_map.get(port_hash_map.get(successor1));
							cursor = fwdQuery(index, selection, "1query");
							return cursor;
						} catch (IOException e1) {
							Log.e(TAG, "1 query IOException while reading from successor 1" + e1.getMessage());
						} catch (NullPointerException e1) {
							Log.e(TAG, "1 query NullPointerException while reading from successor 1" + e1.getMessage());
						}
					} finally {
						semaphore.release(3);
					}
				} else {
					for (int i = 0; i <= 4; i++) {
						if (val.compareTo(ring.get(i)) < 0) {
							if (i == 0) {
								//this node has it
								j = 2;
								break;
							} else if (val.compareTo(ring.get(i - 1)) > 0) {
								//this node has it
								j = (i + 2) % 5;
								break;
							}
						} else if (i == 4 && val.compareTo(ring.get(i)) > 0) {
							// 0th node has it
							j = 2;
							break;
						}
					}
					Log.v(TAG, "Port with data" + port_hash_map.get(ring.get(j)));
					if(!encoded_port.equals(ring.get(j))){
						try{
							try {
								semaphore.acquire(3);
							} catch (InterruptedException e) {
								Log.e(TAG, e.getMessage());
							}
							index = port_socket_map.get(port_hash_map.get(ring.get(j)));
							cursor = fwdQuery(index, selection, "1query");
							return cursor;
						} catch (IOException e) {
							Log.e(TAG, "1 query IOException while reading from j node - " + j + e.getMessage());
							try {
								j = (j + 5 - 1) % 5;
								if(!encoded_port.equals(ring.get(j))) {
									index = port_socket_map.get(port_hash_map.get(ring.get(j)));
									cursor = fwdQuery(index, selection, "1query");
									return cursor;
								} else {
									Log.v(TAG, "Current port has the file" + encoded_port);
									files.add(selection);
								}
							} catch (IOException e1) {
								Log.e(TAG, "1 query IOException while reading from j - 1 node - " + j + e1.getMessage());
							} catch (NullPointerException e1) {
								Log.e(TAG, "1 query NullPointerException while reading from j - 1 node - " + j + e1.getMessage());
							}
						} catch (NullPointerException e) {
							Log.e(TAG, "1 query NullPointerException while reading from j node - " + j + e.getMessage());
							try {
								j = (j + 5 - 1) % 5;
								if(!encoded_port.equals(ring.get(j))) {
									index = port_socket_map.get(port_hash_map.get(ring.get(j)));
									cursor = fwdQuery(index, selection, "1query");
									return cursor;
								} else {
									Log.v(TAG, "Current port has the file" + encoded_port);
									files.add(selection);
								}
							} catch (IOException e1) {
								Log.e(TAG, "1 query IOException while reading from j - 1 node - " + j + e1.getMessage());
							} catch (NullPointerException e1) {
								Log.e(TAG, "1 query NullPointerException while reading from j - 1 node - " + j + e1.getMessage());
							}
						} finally {
							semaphore.release(3);
						}
					} else {
						Log.v(TAG, "Current port has the file" + encoded_port);
						files.add(selection);
					}

				}

			} catch (NoSuchAlgorithmException e){
				Log.e(TAG,"NoSuchAlgorithmException" + e.getMessage());
			}
		} else {
			files.add(selection);

		}
		query_request = false;
		FileInputStream inputStream;
		try {
			fileaccess.acquire(3);
		} catch (InterruptedException e){
			Log.e(TAG, e.getMessage());
		} finally {
			fileaccess.release(3);
		}

		if (selection.equals("@") || selection.equals("*")) {
			files.addAll(Arrays.asList(ctx.fileList()));
		}
		for (String filename: files) {
			StringBuilder string = new StringBuilder();
			try {
				filelock.lock();
				inputStream = ctx.openFileInput(filename);
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				String l;
				while ((l = bufferedReader.readLine()) != null) {
					string.append(l);
				}
				inputStreamReader.close();
				inputStream.close();
			} catch (Exception e) {
				Log.e(TAG, "CP - File read failed - " + filename);
				Log.v(TAG, "Trying again in 100 ms....");
				try {
					Thread.sleep(300);
				} catch(InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				try {
					inputStream = ctx.openFileInput(filename);
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String l;
					while ((l = bufferedReader.readLine()) != null) {
						string.append(l);
					}
					inputStreamReader.close();
					inputStream.close();
				} catch (Exception e1) {
					Log.e(TAG, "CP - File read failed again - " + filename);
				}
			} finally {
				filelock.unlock();
			}

			try {
				cursor.newRow()
						.add("key", filename)
						.add("value", string);

			} catch (Exception ex) {
				Log.e(TAG, "Matrix cursor failed");
			}
		}
		cursor.close();
		return cursor;
	}

	static MatrixCursor fwdQuery(int index, String selection, String query_type) throws IOException {
		String key;
		String value;
		MatrixCursor cursor = new MatrixCursor( new String[] {"key", "value"});
		try {
			s_out[index].writeUTF(query_type);
			s_out[index].flush();
			s_out[index].writeUTF(selection);
			s_out[index].flush();
			String query_return = s_in[index].readUTF();
			if (query_return.equals("SQR")){
				int count = s_in[index].readInt();
				for(int i = 0; i < count; i++){
					key = s_in[index].readUTF();
					value = s_in[index].readUTF();
					cursor.newRow()
							.add("key", key)
							.add("value", value);
				}
				cursor.close();
			}
		} catch (IOException e) {
			throw e;
		} catch (NullPointerException e) {
			throw e;
		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
