package org.wso2.carbon.sample.performance.feedback;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Created by chamod on 8/28/17.
 */
public class TCPSessionWriter extends Thread {

    private Socket connectionSocket;
    private static SiddhiHandler siddhiHandler = new SiddhiHandler();

    public TCPSessionWriter(Socket connectionSocket) {
        this.connectionSocket = connectionSocket;
    }

    @Override
    public void run() {
        BufferedReader inFromClient = null;
        try {
            inFromClient = new BufferedReader(new InputStreamReader(TCPSessionWriter.this.connectionSocket.getInputStream()));

//          DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            String clientSentence = null;

            clientSentence = inFromClient.readLine();

            int punctuation = Integer.valueOf(clientSentence.split(":")[1].trim());

            if (punctuation == -1) {
                Object[] data = {0.0f, 0.0, System.currentTimeMillis(), 0, punctuation};
                siddhiHandler.sendEvent(data);
            }

            System.out.println("Received Punctuation : " + punctuation);
//            FeedbackProcessor.getInstance().handleFeedback(clientSentence);

//          String capitalizedSentence = clientSentence.toUpperCase() + '\n';
//          outToClient.writeBytes(capitalizedSentence);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
