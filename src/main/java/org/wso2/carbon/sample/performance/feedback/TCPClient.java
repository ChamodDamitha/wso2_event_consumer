package org.wso2.carbon.sample.performance.feedback;

import java.io.*;
import java.net.*;

public class TCPClient extends Thread {
    private String host;
    private int port;
    private String msg;
    private DataOutputStream outToServer;
    private Socket clientSocket;

    public TCPClient(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            clientSocket = new Socket(this.host, this.port);
            outToServer = new DataOutputStream(clientSocket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendMsg(String msg) {
        try {
            outToServer.writeBytes(msg + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
//                System.out.println("MSG Sending triggered................");
        try {
//            String sentence;
//            String modifiedSentence;
//            BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
            Socket clientSocket = new Socket(TCPClient.this.host, TCPClient.this.port);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
//            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//                    sentence = msg;
//            sentence = inFromUser.readLine();
            outToServer.writeBytes(msg + "\n");
//            modifiedSentence = inFromServer.readLine();
//                        System.out.println("FROM SERVER: " + modifiedSentence);
            clientSocket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
