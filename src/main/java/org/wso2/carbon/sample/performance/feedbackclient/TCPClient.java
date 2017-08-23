package org.wso2.carbon.sample.performance.feedbackclient;

import java.io.*;
import java.net.*;

public class TCPClient {
    private String host;
    private int port;

    public TCPClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void sendMsg(String msg) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        String sentence;
                        String modifiedSentence;
//            BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
                        Socket clientSocket = new Socket(TCPClient.this.host, TCPClient.this.port);
                        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
                        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        sentence = msg;
//            sentence = inFromUser.readLine();
                        outToServer.writeBytes(sentence + '\n');
                        modifiedSentence = inFromServer.readLine();
//                        System.out.println("FROM SERVER: " + modifiedSentence);
                        clientSocket.close();

                        Thread.sleep(100);

                    } catch (IOException e) {
                    } catch (InterruptedException e) {
                    }
                }
            }
        }).start();

    }
}
