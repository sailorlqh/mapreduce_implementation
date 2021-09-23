

import com.sun.security.ntlm.Server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Random;

public class Client {

    //multithread support for client
    //this class is used to support socket message that client receives
    public static class clientSocketHandler extends Thread {
        Socket socket = null;
        Config conf;
        ServerSocket ss = null;
        boolean flag = true;
        public String fileLocation = null;
        public clientSocketHandler() {
            conf =  new Config();
        }

        public void run() {
            InputStream is = null;
            InputStreamReader isr=null;
            BufferedReader br=null;
            OutputStream os=null;
            PrintWriter pw=null;
            String replyMessage = null;
            try {
                ss = new ServerSocket(conf.getClientPort());
            } catch (IOException e) {
                e.printStackTrace();
            }

            //always ready to receive socket message and get message content from the socket
            while(true) {
                try {
                    System.out.println("client node started, waiting for message");
                    socket = ss.accept();
                    is = socket.getInputStream();
                    isr = new InputStreamReader(is);
                    br = new BufferedReader(isr);
                    String message = null;
                    boolean result = false;
                    if((message = br.readLine()) != null) {
                        System.out.println("Client message Handler received message: " + message);
                        result = parseMessage(message);
                    } else {
                        result = false;
                    }
                    //if client get finish message from master server
                    //print out corrsponding message
                    if(result) {
                        System.out.println("client receives finish message");
                    }
                    socket.shutdownInput();
                    os = socket.getOutputStream();
                    replyMessage = "client has received your message";
                    pw = new PrintWriter(os);
                    pw.write(replyMessage);
                    pw.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        //parse message that client receives
        //return the parsed final file location
        public boolean parseMessage(String message) {
            if (message.contains("Job finished")) {
                String[] tempArgs = message.split(" ", 0);
                fileLocation = tempArgs[tempArgs.length-1];
                System.out.println("parse message result: filelocation=" + fileLocation);
                return true;
            }
            return false;
        }
    }

    //main function, used to test client's functionality
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 34000);
            OutputStream os = socket.getOutputStream();
            PrintWriter pw = new PrintWriter(os);

            for(int i = 0; i < 1; i++){
                //wordcount, ordercount, maxcount
                String url="start/master/file=bbb.txt/job=maxcount";
                pw.write(url);
                pw.flush();
            }
            socket.shutdownOutput();
            InputStream is = socket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String info = null;
            while ((info = br.readLine()) != null) {
                System.out.println("message received:" + info);
            }
            br.close();
            is.close();
            pw.close();
            os.close();
            socket.close();
        } catch (IOException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
//        clientSocketHandler csh = new clientSocketHandler();
//        csh.start();
    }
}