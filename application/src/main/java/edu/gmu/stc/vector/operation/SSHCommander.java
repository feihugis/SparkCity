package edu.gmu.stc.vector.operation;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import ch.ethz.ssh2.ChannelCondition;
/**
 * Created by Yun Li on 2/28/18.
 * One SSHCommander instance is one connection to one machine.
 */

public class SSHCommander {

	private String hostname;
	private String username;
	private File pemFile;
	private String password;
	private Connection conn;
	private String mode;
	
	public String getHostname() {
		return this.hostname;
	}
	public String getUsername() {
		return this.username;
	}
	public Connection getConn() {
		return this.conn;
	}

	public SSHCommander(String hostname, String username, String pemOrpwd){
		this.hostname = hostname;
		this.username = username;
		if(pemOrpwd.endsWith(".pem")) {
			this.pemFile = new File(pemOrpwd);
		}else{
			this.password = pemOrpwd;
		}
		try {
			Connection conn = new Connection(hostname);
			conn.connect();
			boolean isAuthenticated = false;
			if(pemOrpwd.endsWith(".pem")) {
				isAuthenticated = conn.authenticateWithPublicKey(username, this.pemFile, null);
			}else{
				isAuthenticated = conn.authenticateWithPassword(username, this.password);
			}

			if (isAuthenticated == true) {
				this.conn = conn;
			} else {
				this.conn = null;
				throw new IOException("Authentication failed.");
			}
		} catch (IOException e) {
			e.printStackTrace(System.err);
			this.conn = null;
		}

		this.mode = "0644";
	}
	
	public void closeConnection(){
		this.conn.close();
	}

	public ArrayList<String> executeCommand(String cmd){
		ArrayList<String> stdoutStr = new ArrayList<String>();
		try {
			Session sess = this.conn.openSession();
			sess.execCommand(cmd);
			InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			while (true)
			{
				String line = br.readLine();
				if (line == null)
					break;
				System.out.println(line);
				stdoutStr.add(line);
			}
			System.out.println("###SshOperator.executeCommand:"+cmd+" ; ExitCode: " + sess.getExitStatus());
			//System.out.println("-------------------------------------------------");
			sess.close();
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
		return stdoutStr;
	}

	public void copyFileToRemote(String localFile, String remoteDir) throws IOException{

		SCPClient scp = this.conn.createSCPClient();
		File file = new File(localFile);
		if (file.isFile()) {
			scp.put(localFile, remoteDir,"0644");
		}
		//System.out.println("complete scp file to remote machine" + scp.toString());
	}

	public String copyDirToRemote(String localDir, String remoteDir) throws IOException {
		File curDir = new File(localDir);
		if (curDir.isFile()) {
			this.copyFileToRemote(localDir, remoteDir);
			return remoteDir + curDir.getName();
		}

		SCPClient scp = this.conn.createSCPClient();
		String dirName = curDir.getName();
		remoteDir += dirName + "/";
		executeCommand("mkdir " + remoteDir);
		final String[] fileList = curDir.list();
		for (String file : fileList) {
			System.out.println("copy file to remote machine: " + file);
			final String fullFileName = localDir + "/" + file;
			if (new File(fullFileName).isDirectory()) {
				this.copyDirToRemote(fullFileName, remoteDir);
			}
			else {
				scp.put(fullFileName, remoteDir, this.mode);
			}
		}

		return remoteDir;
	}
}