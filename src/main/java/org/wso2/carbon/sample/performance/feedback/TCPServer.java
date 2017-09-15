package org.wso2.carbon.sample.performance.feedback;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer extends Thread{
    private Socket connectionSocket;
    private int port;

    public TCPServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            System.out.println("PUNCTUATION SERVER started................");
            ServerSocket welcomeSocket = new ServerSocket(TCPServer.this.port);
            while (true) {
                connectionSocket = welcomeSocket.accept();
                new TCPSessionWriter(connectionSocket).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
