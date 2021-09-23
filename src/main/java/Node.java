

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Node{

    /**
     * thread that handle socket incoming messages
     */
    public class SocketMessageHandler extends Thread {
        Socket socket = null;

        /**
         * parsemessage so that different message can be handled differently
         * @param message
         * @return  status code
         * @throws IOException
         */
        public String parseMessage(String message) throws IOException, InterruptedException {
            //master receives message, start all mapreduce message
            //format: start/master/file=<>/job=<>
            if(message.contains("start/master")) {
                String result = parseMasterStartMessage(message);
                return result;
            }

            //worker receives message
            // assign role to worker (mapper or reducer)
            //format: assignrole/<role>/<job name>
            if(message.contains("assignrole")) {
                String result = parseWorkerAssignMessage(message);
                return result;
            }

            //worker receives message
            //master tells mapper to start working
            //format: start/worker/mapper/file=<file name>/startline=<start line num>/endline=<end line num>/partitionId=<partition id>
            if(message.contains("start/worker/mapper")) {
                String result = parseMapperStartMessage(message);
                return result;
            }

            //master receives this message
            //mapper finish it's task, send message to nofify master
            //format: mapper-task-finish/id=<>/file=<aaa.txt>
            if(message.contains("mapper-task-finish")) {
                String result = parseMapperTaskFinishMessage(message);
                return result;
            }

            //worker receives this message
            //master tells reducer to start working
            //format: start/worker/reduce/mapperId=<>/filePath=<>
            if(message.contains("start/worker/reducer")) {
                String result = parseReducerStartMessage(message);
                return result;
            }

            //master receives this message
            //reducer tells master job is finished
            //format: reducer-task-finish/id=<>/file=<aaa.txt>
            if(message.contains("reducer-task-finish")) {
                String result = parseReducerTaskFinishMessage(message);
                return result;
            }

            //heartbest message
            //format: heartbeat
            if(message.contains("heartbeat")) {
                String result = parseHeartbeatMessage(message);
                return result;
            }
            return "invalid input parameters";
        }

        public SocketMessageHandler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            InputStream is = null;
            InputStreamReader isr=null;
            BufferedReader br=null;
            OutputStream os=null;
            PrintWriter pw=null;
            String replyMessage = null;
            try{
                is = socket.getInputStream();
                isr = new InputStreamReader(is);
                br = new BufferedReader(isr);
                String message = null;
                if((message = br.readLine()) != null) {
                    System.out.println("SocketMessageHandler received message: " + message);
                    replyMessage = parseMessage(message);
                } else {
                    replyMessage = "Didn't receive messgae";
                }
                socket.shutdownInput();
                os = socket.getOutputStream();

                pw = new PrintWriter(os);
                pw.write(replyMessage);
                pw.flush();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                try {
                    if(pw!=null)
                        pw.close();
                    if(os!=null)
                        os.close();
                    if(br!=null)
                        br.close();
                    if(isr!=null)
                        isr.close();
                    if(is!=null)
                        is.close();
                    if(socket!=null)
                        socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //format: start/master/file=bbb.txt/job=task1
    public String parseMasterStartMessage(String message) throws IOException {
        String[] args = message.split("/", -1);
        System.out.println(args.length);
        for(String temp:args) {
            System.out.println(temp);
        }
        if (args.length != 4) {
            return "invalid parameters";
        } else {
            String file = args[2];
            String job = args[3];
            String filePath = args[2].split("=")[1];
            String jobName = args[3].split("=")[1];
            ms.setTxtFilePath(filePath);
            boolean result = ms.assignWorker(jobName);
            if(!result) {
                return "There isn't enough server available for the mapreduce task";
            }
            return "Worker Assigned Successfully, start doing mapreduce";
        }
    }

    //format: start/worker/mapper/file=aaa.txt/startline=0/endline=1/partitionId=2
    public String parseMapperStartMessage(String message) throws InterruptedException {
        String[] args = message.split("/", -1);
        if(args.length != 7) {
            System.out.println("invalid parameters");
            return "invalid parameters";
        } else {
            String file = args[3].split("=", -1)[1];
            int startline = Integer.valueOf(args[4].split("=", -1)[1]);
            int endline = Integer.valueOf(args[5].split("=", -1)[1]);
            int partitionId = Integer.valueOf(args[6].split("=", -1)[1]);
            String result = ws.startMapPhase(file, startline, endline, partitionId);
            return result;

        }
    }

    //format: assignrole/<role>/<job>
    public String parseWorkerAssignMessage(String message) {
        String[] args = message.split("/", -1);
        if(args.length != 3) {
            System.out.println("invalid parameters");
            return "invalid parameters";
        } else {
            String role = args[1];
            String job = args[2];
            String result = ws.assignWorkerRole(role, job);
            return result;
        }
    }

    //format: mapper-task-finish/id=<>/file=<aaa.txt>
    //todo: now only support one mapper
    public String parseMapperTaskFinishMessage(String message) {
        String[] args = message.split("/", -1);
        if(args.length != 3) {
            System.out.println("invalid parameters");
            return "Invalid parameters";
        } else {
            int id = Integer.valueOf(args[1].split("=", -1)[1]);
            String filePath = args[2].split("=", -1)[1];
            String result = ms.receiveMapperFinishMessage(id, filePath);
            return result;
        }
    }

    //format: reducer-task-finish/id=<>/file=<aaa.txt>
    public String parseReducerTaskFinishMessage(String message) {
        String[] args = message.split("/", -1);
        if(args.length != 3) {
            System.out.println("invalid parameters");
            return "Invalid parameters";
        } else {
            int id = Integer.valueOf(args[1].split("=", -1)[1]);
            String filePath = args[2].split("=", -1)[1];
            String result = ms.receiveReducerFinishMessage(id, filePath);
            return result;
        }
    }


    //format: start/worker/reduce/mapperId=<>/hashCode=<>/filePath=<>
    public String parseReducerStartMessage(String message) throws InterruptedException {
        String[] args = message.split("/", -1);
        if(args.length != 6) {
            System.out.println("invalid parameters");
            return "Invalid parameters";
        } else {
            int id = Integer.valueOf(args[3].split("=", -1)[1]);
            int hashCode = Integer.valueOf(args[4].split("=", -1)[1]);
            String file = args[5].split("=", -1)[1];
            String result = ws.startReducePhase(file, hashCode);
            return result;
        }
    }

    //format: heartbeat
    public String parseHeartbeatMessage(String message) {
        int stage = Integer.valueOf(message.split("/", 0)[1]);
        if(ws != null && ws.getWorkerId() == conf.failWorkerId && stage == conf.failStage) {
            System.out.println("System Down, accoding to config");
            return "500";
        } else {
            return "200";
        }
    }

    MasterServer ms = null;
    WorkerServer ws = null;
    private static int nodeRole;
    private static int WorkerRole;
    public ServerSocket ss = null;
    public Config conf = null;

    /**
     * init node parameters
     * @param port type:int, node's port number
     * @param NodeRole type:int, node's role
     * @param id type:int, node's id
     */
    public Node(int port, int NodeRole, int id) {
        conf = new Config();
        nodeRole = NodeRole;
        if(nodeRole == conf.Master){
            System.out.print("=====================\nRole: Master\n");
            System.out.print("Port:" + port + "\n=====================\n");
            ms = new MasterServer();
        } else if(nodeRole == conf.Worker) {
            System.out.print("=====================\nRole: Worker\n");
            System.out.print("Port:" + port + "\n=====================\n");
            ws = new WorkerServer(id);
        } else {
            System.out.println("Invalid Node Type");
        }
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        startNode();
    }

    /**
     * start node to listen to incoming socket message
     */
    public void startNode() {
        try{
            Socket socket = null;
            System.out.println("**Node Started, waiting for message**");
            while(true) {
                socket = ss.accept();
                SocketMessageHandler smh = new SocketMessageHandler(socket);
                smh.start();
                InetAddress address=socket.getInetAddress();
                System.out.println("client IP："+address.getHostAddress());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if(args.length != 3) {
            System.out.println("Invalid input, three parameters are required\n command format: <port number> <role> <id>");
        }
        int port = Integer.parseInt(args[0]);
        int role = Integer.parseInt(args[1]);
        int id = Integer.parseInt(args[2]);
        Node node = new Node(port, role, id);
    }
}