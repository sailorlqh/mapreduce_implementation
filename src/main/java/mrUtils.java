import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

public class mrUtils {
    /**
     * tuple, used in wordcount job
     * @param <A>
     * @param <B>
     */
    public static class TwoTuple<A, B> {

        public final A first;

        public final B second;

        public TwoTuple(A a, B b){
            first = a;
            second = b;
        }

        public String toString(){
            return "(" + first + ", " + second + ")";
        }

        public A getFirst() {
            return first;
        }

        public B getSecond() {
            return second;
        }

    }

    /**
     * three tuple, used in ordercount and maxvalue
     * @param <A>
     * @param <B>
     * @param <C>
     */
    public static class ThreeTuple<A, B, C> extends TwoTuple<A, B> {
        public final C third;

        public ThreeTuple(A a, B b, C c) {
            super(a, b);
            this.third = c;
        }

        public C getThird() { return third;}
    }

    public static class sendStartMessageThread extends Thread{
        public int pn;
        public String mess;
        public sendStartMessageThread(int portNumber, String message){
            pn = portNumber;
            mess = message;
        }
        public void run() {
            System.out.println("send message");
            sendMessageHandler(pn, mess);
        }
    }

    /**
     * start socket and send message to the desired port
     * @param portNumber type:int, port number that message was send to
     * @param message type:String, message itself
     * @return type:String send success or not
     */
    public static String sendMessage(int portNumber, String message) {
        sendStartMessageThread ssmt = new sendStartMessageThread(portNumber, message);
        ssmt.start();
        return "200";
    }

    /**
     * send message to desired port
     * @param portNumber port number to send the message
     * @param message mesaage content
     * @return
     */
    public static String sendMessageHandler(int portNumber, String message) {
        try {
            Socket socket = new Socket("localhost", portNumber);
            OutputStream os = socket.getOutputStream();
            PrintWriter pw = new PrintWriter(os);

            pw.write(message);
            pw.flush();

            socket.shutdownOutput();
            InputStream is = socket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String info = null;
            String ans = "";
            while ((info = br.readLine()) != null) {
                System.out.println("message received:" + info);
                ans += info;
            }
            br.close();
            is.close();
            pw.close();
            os.close();
            socket.close();
            return ans;
        } catch (IOException ex) {
            System.out.println("message send failed");
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            return "failed to send message";
        }
    }

    /**
     * shut down master and all worker servers
     * @throws IOException
     */
    public static void shutDownServers() throws IOException {
        System.out.println("cleaner");
        Process p = Runtime.getRuntime().exec("jps");
        String line = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(
                p.getInputStream(), "UTF-8"));

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

    /**
     * test script for task1 (wordcount)
     * @param validFile type:String, groundtruth file dir
     * @param allTestFiles type:String, result file dir, format: file1&file2&...
     * @return
     */
    public static boolean testTask1(String validFile, String allTestFiles){
        System.out.println(validFile);
        HashMap<String, Integer> valid = new HashMap<>();
        HashMap<String, Integer> test = new HashMap<>();

        //read content from file validFile, store them in hashmap
        try {
            BufferedReader buf = new BufferedReader(new FileReader(validFile));
            String line = buf.readLine();
            while (line != null) {
                if(line.length() == 0) {
                    line = buf.readLine();
                    continue;
                }
                System.out.println(line);
                String temp = line.substring(1, line.length()-1);
                System.out.println("temp + " + temp);
                String[] vars = temp.split(", ", 0);
                String key = vars[0];
                String tempKey = key;
                if(key.charAt(0) == '\'' || key.charAt(0) == '"')  tempKey = key.substring(1, key.length()-1);
                int count = Integer.valueOf(vars[1]);
                valid.put(tempKey, Integer.valueOf(vars[1]));
                line = buf.readLine();
            }
            buf.close();
        } catch (Exception e) {
            System.out.println("phase 1");
            e.printStackTrace();
        }

        //read content from all test files, store them in hashmap
        String[] allTestFile = allTestFiles.split("&");
        for(String testFile:allTestFile) {
            try {
                BufferedReader testbuf = new BufferedReader(new FileReader(testFile));
                String line = testbuf.readLine();
                while (line != null) {
                    if (line.length() == 0) {
                        line = testbuf.readLine();
                        continue;
                    }
                    System.out.println(line);
                    String temp = line.substring(1, line.length() - 1);
                    String[] vars = temp.split(", ", 0);
                    String key = vars[0];
                    int count = Integer.valueOf(vars[1]);
                    test.put(key, Integer.valueOf(vars[1]));
                    line = testbuf.readLine();
                }
                testbuf.close();
            } catch (Exception e) {
                System.out.println("phase 1");
                e.printStackTrace();
            }
        }

        //compare two hashmap, check if them have same key set
        //and check each key has the same value
        for(String s : valid.keySet()) {
            if(valid.containsKey(s) && test.containsKey(s)) {

            } else {
                System.out.println("Does not have same keys");
                System.out.println(s);
                if(valid.containsKey(s)) {
                    System.out.println("valid");
                }
                if(test.containsKey(s)) {
                    System.out.println("test");
                }
                return false;
            }

            if(String.valueOf(valid.get(s)).equals(String.valueOf(test.get(s)))){
                continue;
            } else {
                System.out.println("Key: " + s);
                System.out.println(valid.get(s));
                System.out.println(test.get(s));
                return false;
            }
        }
        return true;
    }

    /**
     * test script for task2 (orderCount)
     * almost the same as task 1
     * @param validFile type:String, groundtruth file dir
     * @param allTestFiles type:String, result file dir, format: file1&file2&...
     * @return
     */
    public static boolean testTask2(String validFile, String allTestFiles){
        System.out.println(validFile);
        HashMap<String, ArrayList<String>> valid = new HashMap<>();
        HashMap<String, ArrayList<String>> test = new HashMap<>();

        //read from valid file, store in hashmap
        try {
            BufferedReader buf = new BufferedReader(new FileReader(validFile));
            String line = buf.readLine();
            while (line != null) {
                if(line.length() == 0) {
                    line = buf.readLine();
                    continue;
                }
                System.out.println(line);
                String temp = line.substring(1, line.length()-1);
                String[] vars = temp.split(", ", 2);
                String key = vars[0];
                String temp_var = vars[1].substring(1, vars[1].length()-1);
                String [] value = temp_var.split(", ",0);
                ArrayList<String> temp_v = new ArrayList<String>();
                for(int i=0;i<value.length;i++){
                    temp_v.add(value[i]);
                }
                Collections.sort(temp_v);
                valid.put(key, temp_v);
                line = buf.readLine();
            }
            buf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //read from all test file, store in hashmap
        String[] allTestFile = allTestFiles.split("&");
        for(String testFile:allTestFile) {
            try {
                BufferedReader testbuf = new BufferedReader(new FileReader(testFile));
                String line = testbuf.readLine();
                while (line != null) {
                    if (line.length() == 0) {
                        line = testbuf.readLine();
                        continue;
                    }
                    System.out.println(line);
                    String temp = line.substring(1, line.length() - 1);
                    String[] vars = temp.split(", ", 2);
                    String key = vars[0];
                    String temp_var = vars[1].substring(1, vars[1].length() - 1);
                    String[] value = temp_var.split(", ", 0);
                    ArrayList<String> temp_t = new ArrayList<String>();
                    for (int i = 0; i < value.length; i++) {
                        temp_t.add(value[i]);
                    }
                    Collections.sort(temp_t);
                    test.put(key, temp_t);
                    line = testbuf.readLine();
                }
                testbuf.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //compare two hashmap, check if them have same key set
        //and check each key has the same value
        for(String s : valid.keySet()) {
            if(valid.containsKey(s) && test.containsKey(s)) {

            } else {
                System.out.println("Does not have same keys");
                System.out.println(s);
                if(valid.containsKey(s)) {
                    System.out.println("valid");
                }
                if(test.containsKey(s)) {
                    System.out.println("test");
                }
                return false;
            }

            if(valid.get(s).equals(test.get(s))){
                continue;
            } else {
                System.out.println("Key: " + s);
                System.out.println(valid.get(s));
                System.out.println(test.get(s));
                return false;
            }
        }
        return true;
    }

    /**
     * test script for task3 (maxvalue)
     * same as testTask1
     * @param validFile type:String, groundtruth file dir
     * @param allTestFiles type:String, result file dir, format: file1&file2&...
     * @return
     */
    public static boolean testTask3(String validFile, String allTestFiles){
        return testTask1(validFile, allTestFiles);
    }

    public static void main(String[] args) {
        boolean result = testTask2("ordercount_result.txt", "ordercount_final_result.txt");
        if(result) {
            System.out.println("hahaha");
        }
    }
}
