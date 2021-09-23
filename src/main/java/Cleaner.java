package main.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

//This class is used to run jps command, can kill all the processes we lunched as master, client, mapper and reducer
public class Cleaner {
    public static void main(String[] args) throws IOException {
        Process p = Runtime.getRuntime().exec("jps");
        String line = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(
                p.getInputStream(), "UTF-8"));

        //filter each line that contains "Node" or "Client get their pid
        //and than kill that process using kill command
        while ((line = in.readLine()) != null) {
            String [] javaProcess = line.split(" ");
            if (javaProcess.length > 1 && (javaProcess[1].endsWith("Node") || javaProcess[1].endsWith("Client"))) {
                System.out.println(line);
                String cmd = "kill " + javaProcess[0];
                System.out.println(cmd);
                Process tempP = Runtime.getRuntime().exec(cmd);
            }
        }
    }
}
