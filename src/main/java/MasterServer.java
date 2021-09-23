import java.io.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;

public class MasterServer {
    public static int server_port; //master server's port nu,ber
    public static int num_workers; //number of workers
    public static int num_mappers; //number of mappers
    public static int num_reducers; //number of reducers
    public static int client_port; //client's port number
    public static HashMap<Integer, Integer> workerPorts; //<serverID, serverPort>
    public static boolean[] crashed; //if the server is crashed
    public static ArrayList<Integer> map_worker_list;
    public static ArrayList<Integer> reduce_worker_list;
    public static ArrayList<Integer> idel_worker_list;
    public static String jarfilePath;
    public static String txtFilePath;
    public static String job;
    public static HashMap<Integer, Integer> partitionToServerMapTable; //<PartitionId, ServerId>
    public static HashMap<Integer, Integer> serverToPartitionMapTable; //<ServerId, PartitionId>
    public static HashMap<Integer, String> partitionToMapperFilenameMapTable; //<PartitionId, Partition-intermidiate-file-name>
    public static ArrayList<String> filePathForReducers;
    public static ConcurrentHashMap<Integer, Boolean> mapperPartitionStatus;
    public static ConcurrentHashMap<Integer, Boolean> reducerFinishStatus;
    public static int sendStartReduceMessageFlag;
    public static int sendClientResponseMessageFlag;
    private static Lock mapperStatusLock;
    private static Lock reducerStatusLock;
    public static ArrayList<String> mapperGenerateResultFilePath;
    public static ArrayList<String> reducerGenerateResultFilePath;
    public static Config conf;
    public static HashMap<Integer, Integer> reducerHashCode; //<Hashcode, server-id>
    public static HashMap<Integer, mrUtils.TwoTuple<Integer, Integer>> partitionStartAndEndLineDict; //<partitionId, <startline, endline>

    public static int stage;
//    private static OkHttpClient httpClient = new OkHttpClient();

    /**
     * init
     */
    public MasterServer(){
        conf = new Config();
        server_port = conf.getMasterPort();
        System.out.println();
        workerPorts = conf.getWokerPort();
        num_workers = conf.getNumWorkers();
        num_mappers = conf.getNumMapper();
        num_reducers = conf.getNumReducer();
        crashed = new boolean[num_workers + 1];


        map_worker_list = new ArrayList<>();
        reduce_worker_list = new ArrayList<>();
        idel_worker_list = new ArrayList<>();

        client_port = conf.getClientPort();
        partitionToServerMapTable = new HashMap<>();
        serverToPartitionMapTable = new HashMap<>();
        mapperPartitionStatus = new ConcurrentHashMap<>();
        reducerFinishStatus = new ConcurrentHashMap<>();
        partitionStartAndEndLineDict = new HashMap<>();
        partitionToMapperFilenameMapTable = new HashMap<>();

        sendStartReduceMessageFlag = 0;
        sendClientResponseMessageFlag = 0;
        stage = 0;

        mapperStatusLock = new ReentrantLock();
        reducerStatusLock = new ReentrantLock();

        mapperGenerateResultFilePath = new ArrayList<>();
        reducerGenerateResultFilePath = new ArrayList<>();

        reducerHashCode = new HashMap<>();
        filePathForReducers = new ArrayList<String>();
    }

    public static void setJarFilePath(String path){
        jarfilePath = path;
    }

    public static void setTxtFilePath(String path){
        txtFilePath = path;
    }

    public static void setJobName(String jn) {job = jn;}

    /**
     * assign worker with its role
     * @return boolean, true if all workers can be assigned, and can do map task
     * @throws IOException
     */
    public static boolean assignWorker(String job) throws IOException {
        setJobName(job);
        int id = 1;
        int count = 0;
        //iterate each worker server,
        //if it isn't a crashed server,
        //assign a mapper's role to it
        while (count < num_mappers && id < num_workers+1){
            if(!crashed[id]){
                sendAssignMessage(id, "mapper", job);
                map_worker_list.add(id);
                count += 1;
            }
            id += 1;
        }
        count = 0;
        int tempHash = 0;

        //iterate each un-used worker server,
        //if it isn't a crashed server
        //assign a reduer's role to it
        //and assign a hashcode to it
        //this hashcode indicates which key this reducer need to parse
        while(count < num_reducers && id < num_workers+1) {
            if(!crashed[id]) {
                sendAssignMessage(id, "reducer", job);
                reduce_worker_list.add(id);
                reducerHashCode.put(tempHash, id);
                reducerFinishStatus.put(id, false);
                count += 1;
                tempHash += 1;
            }
            id += 1;
        }

        //no enough available worker
        if(id > num_workers+1){
            System.out.println("There isn't enough server available for the mapreduce task");
            return false;
        } else{
            //if all workers can be assigned
            //store remaining available server's id
            //so that when fault occurs, they can be assigned a role
            //and start doing job
            while(id < num_workers + 1) {
                idel_worker_list.add(id);
                id += 1;
            }
            //if all workers can be assigned
            //start doing mapreduce job
            System.out.println("Master finishes assigning role");
            System.out.println(map_worker_list.size());
            stage = 1;
            heartbeatTaskThread hbtt = new heartbeatTaskThread();
            hbtt.start();
            simplePartition();
            return true;
        }
    }

    /**
     * get file line nums for partition use
     * @return int, file line nums
     * @throws IOException
     */
    public static int getFileLineNum() throws IOException {
        LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(txtFilePath)));
        lnr.skip(Long.MAX_VALUE);
        int lineNum = lnr.getLineNumber() + 1;
        System.out.println(lineNum);
        lnr.close();
        return lineNum;
    }

    /**
     * Do partition to the file and send file to mapper
     * so mapper can start doing the task
     */
    public static void simplePartition() {
        System.out.println("File is stored on " + txtFilePath);
        int numLines = 0;
        try{
            numLines = getFileLineNum();
        } catch (IOException e) {
            System.out.println("Failed to load file");
        }
        System.out.println(numLines);

        //simply divide the file into several partitions with equal lines
        //and send these info to the mapper so that mapper can start working
        int start = 0;
        int end = 0;
        int offset = numLines / num_mappers + 1;
        int partitionId = 0;
        for(Integer id:map_worker_list) {
            System.out.println("Master In SimplePartition");
            System.out.println(id);
            try{
                if((start + offset) >= numLines)
                    end = numLines;
                else
                    end = start+offset;
                partitionStartAndEndLineDict.put(partitionId, new mrUtils.TwoTuple<>(start, end));
                sendRequestToMapper(id, start, end, partitionId);
                partitionToServerMapTable.put(partitionId, id);
                serverToPartitionMapTable.put(id, partitionId);
                mapperPartitionStatus.put(id, false);
                start = start + offset + 1;
                partitionId += 1;
            } catch (IOException e) {
                System.out.println("Fail to send message");
            }
        }

    }

    /**
    * master receive mapper finish message, send message to reducer to start reducing phase
    * @param mapperServerId, int, serverid that reducer need to contact
     * @param  filePath, String, filename that reducer need to get
     **/
    public static String receiveMapperFinishMessage(int mapperServerId, String filePath) {
        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss a");// a为am/pm的标记
        Date date = new Date();// 获取当前时间
        System.out.println("Received Mapper finish message " + sdf.format(date));
        int count = 0;
        mapperStatusLock.lock();
        mapperGenerateResultFilePath.add(filePath);

        //set a mapper's finish status as true
        mapperPartitionStatus.put(mapperServerId, true);
        partitionToMapperFilenameMapTable.put(serverToPartitionMapTable.get(mapperServerId), filePath);

        //check if all mapper's has finished their job
        //if there exists mapper haven't finish it's job
        //return 200
        for(Integer i : mapperPartitionStatus.keySet()){
            count += 1;
            if(mapperPartitionStatus.get(i) == false){
                mapperStatusLock.unlock();
                System.out.println("server " + i + "haven't finish yet");
                return "200, there are still unfinished mapper jobs";
            }
        }

        //if all mapper's has finished their job
        //start the reduce phase
        if(sendStartReduceMessageFlag == 0 && count == num_mappers) {
            System.out.println("All mapper job finished, now start doing reducing");
            stage = 2;
            String finalFilePath = "";

            //calculate which files should which reducer use
            //put them in the arraylist, format:
            //{
            //     filesforreducer1,
            //     filesforreducer2,
            //     ...
            // }
            //filepath format: <file1>&<file2>&...
            for(int i = 0; i < mapperGenerateResultFilePath.size(); i++) {
                finalFilePath += mapperGenerateResultFilePath.get(i);
                if(i != mapperGenerateResultFilePath.size()-1) finalFilePath += "&";
            }

            //send message to reducer, so that reducer can start working
            simpleSendStartMessageToReducer(mapperServerId, finalFilePath);
            sendStartReduceMessageFlag = 1;
        }
        mapperStatusLock.unlock();
        return "200";
    }

    /**
     * when master receives reducer finish message, this function is used
     * and master will send message to client to tell it all mapreduce job has finished
     * @param reducerServerId type: int
     * @param filePath type: String, the final result path
     * @return 200 status code
     */
    public static String receiveReducerFinishMessage(int reducerServerId, String filePath) {
        System.out.println("Recevied reducer finish messagel, file is stored on " + filePath);
        int count = 0;

        //set a lock, for multi-thread variable safety
        reducerStatusLock.lock();
        reducerGenerateResultFilePath.add(filePath);

        //set reducer's finish status as true
        reducerFinishStatus.put(reducerServerId, true);

        //check if all reducer's has finished
        for(int i:reducerFinishStatus.keySet()) {
            count += 1;
            if(reducerFinishStatus.get(i) == false) {
                reducerStatusLock.unlock();
                System.out.println("Reducer " + i + "haven't finish yet");
                return "200, there are still unfinished reducer jobs";
            }
        }

        //sendClientResponseMessageFlag: use to control that only one finish message
        //will be send to the client
        if(sendClientResponseMessageFlag == 0 && count == num_reducers) {
            System.out.println("All reducer job finished, now send finish message to client");
            String finalFilePath = "";
            for(int i = 0; i < reducerGenerateResultFilePath.size(); i++) {
                finalFilePath += reducerGenerateResultFilePath.get(i);
                if(i != reducerGenerateResultFilePath.size()-1) finalFilePath += "&";
            }
            sendFinishToClient(finalFilePath);
            sendClientResponseMessageFlag = 1;
        }
        reducerStatusLock.unlock();
        return "200";
    }

    /**
     * send finish message to client
     * @param filePath type:String, the final result size
     */
    public static void sendFinishToClient(String filePath) {
        String ans = "Job finished, file is stored on " + filePath;
        System.out.println("Start to send finish message to client");
        mrUtils.sendMessage(client_port, ans);
        System.out.println("finish message to client sent");
    }

    /**
     * send get to mapper so mapper can start doing task
     * @param server_id type:int, serverId of the node that need to do mapping
     * @param start_line type:int, start line number that mapper need to start reading
     * @param end_line type:int, end line number that mapper stops reading here
     * @param partitionId type:int partitionId of the part map-worker uses
     * @throws IOException
     */
    public static void sendRequestToMapper(int server_id, int start_line, int end_line, int partitionId) throws IOException {
        String url = "start/worker/mapper/file=" + txtFilePath + "/startline=" + start_line + "/endline=" + end_line + "/partitionId=" + partitionId;
        mrUtils.sendMessage(workerPorts.get(server_id), url);
    }

    /**
     * send get to worker to assign worker's role and job
     * @param server_id, type: int, server_id
     * @param role, type: String, Mappper/Reducer
     * @param job, type: String, task name of the mapper
     * @throws IOException
     */
    public static void sendAssignMessage(int server_id, String role, String job) throws IOException {
        String url = "assignrole/" + role + "/" + job;
        mrUtils.sendMessage(workerPorts.get(server_id), url);
    }

    /**
     * send start message to reducer to start reduce phase
     * @param id type:int, node server id
     * @param filePath type: file path
     */
    public static void simpleSendStartMessageToReducer(int id, String filePath) {
        System.out.println("Master in simpleSendStartMessageToReducer");
        System.out.println(filePath);
        String[] temp = filePath.split("&",0);

        //convert each filepath into desired format so that reducer can use
        //format: <file1>&<file2>&...
        for(int i = 0; i < conf.getNumReducer(); i ++)
            filePathForReducers.add("");
        for(String s:temp) {
            System.out.println(s);
            int tempHashCode = Integer.valueOf(s.split("_")[0].split("-")[3]);
            if(filePathForReducers.get(tempHashCode).length() == 0) {
                filePathForReducers.set(tempHashCode, filePathForReducers.get(tempHashCode) + s);
            } else {
                filePathForReducers.set(tempHashCode, filePathForReducers.get(tempHashCode) +"&"+ s);
            }
        }

        //send message to each reducer
        for(int i = 0; i < conf.getNumReducer(); i++) {
            try{
                sendReducerStartMessage(reducerHashCode.get(i), id, filePathForReducers.get(i), i);
            } catch (IOException e) {
                System.out.println("failed to send message to reducer");
            }
        }
    }


    /**
     * send meaage to reducer
     * @param reducerId reducerId
     * @param mapperId mapperID
     * @param filePath filePath that reducer needs to get
     * @param hashCode which hashcode the reducers is responsible for
     * @throws IOException
     */
    public static void sendReducerStartMessage(int reducerId, int mapperId, String filePath, int hashCode) throws IOException {
        System.out.println("mapper result stored on "  + filePath);
        String url = "start/worker/reducer/mapperId=" + mapperId + "/hashCode=" + hashCode +"/filePath=" + filePath;
        System.out.println("Master send start-message to reducer");
        mrUtils.sendMessage(workerPorts.get(reducerId), url);
    }

    /**
     * multi-thread class fo heartbeat, used to detect server failur
     * this thread is set to start the hearbeat task every second
     */
    public static class heartbeatTaskThread extends Thread{
        public heartbeatTaskThread(){
        }
        public void run() {
            while(true) {
                try {
                    System.out.println("Sending Heartbeat");
                    startHeartBeatTask();
                    sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * heartbeatMessageHandler for multi-server
     * each handler will be responsible for sending and parsing heartbeat message
     * to each worker server.
     * If worker failure is detected, recovery will be called
     */
    public static class heartbeatMessageHanlder extends Thread{
        public int pn;
        public int serverId;
        public heartbeatMessageHanlder(int id, int portNumber){
            pn = portNumber;
            serverId = id;
        }
        public void run() {
            System.out.println("send message");
            String ans = mrUtils.sendMessageHandler(pn, "heartbeat/" + stage);
            System.out.println("heartbeat return message: " + ans);

            //if worker failed, faultHandler will be called to recover to fault
            if(ans.equals("500")) {
                try {
                    System.out.println("ServerId: " + serverId + " is down");
                    System.out.println("Start fixing");
                    faultHandler(serverId);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("ServerId: " + serverId +" is good");
            }
        }
    }

    /**
     * send heartbeat message to each worer
     */
    public static void startHeartBeatTask() {
        for(Integer id : workerPorts.keySet()) {
            //start a new thread to send heartbeat
            heartbeatMessageHanlder hbmh = new heartbeatMessageHanlder(id, workerPorts.get(id));
            hbmh.start();
        }
    }

    /**
     * handle fault
     * @param serverId failed server's id
     * @throws IOException
     * @throws InterruptedException
     */
    public static void faultHandler(int serverId) throws IOException, InterruptedException {
        //still during the mapping phase
        if(stage == 1) {
            if(isMapper(serverId)) {
                stage1MapperFailure(serverId, 1);
                return;
            }
            if(isReducer(serverId)) {
                System.out.println("Stage 1 reducer fail");
                stage1ReducerFailure(serverId);
                return;
            }
            isIdle(serverId);
        }
        //during the reducing phase
        if(stage == 2) {
            if(isMapper(serverId)) {
                stage2MapperFailure(serverId);
                return;
            }
            if(isReducer(serverId)) {
                stage2ReducerFailure(serverId);
                return;
            }
            isIdle(serverId);
        }
    }

    /**
     * check if a worker's role is mapper
     * @param serverId
     * @return
     */
    public static boolean isMapper(int serverId) {
        for(int i = 0; i < map_worker_list.size(); i++) {
            if(map_worker_list.get(i) == serverId) {
                return true;
            }
        }
        return  false;
    }

    /**
     * check if a worker's role is reducer
     * @param serverId
     * @return
     */
    public static boolean isReducer(int serverId) {
        for(int i = 0; i < reduce_worker_list.size(); i++) {
            if(reduce_worker_list.get(i) == serverId)
                return true;
        }
        return false;
    }

    /**
     * check if a worker is idle
     * @param serverId
     * @return
     */
    public static boolean isIdle(int serverId) {
        for(int i = 0; i < idel_worker_list.size(); i++) {
            if(reduce_worker_list.get(i) == serverId) {
                idel_worker_list.remove(i);
                return true;
            }
        }
        return false;
    }

    /**
     * handle mapper's failure
     * @param serverId
     * @param tempStage
     * @throws IOException
     * @throws InterruptedException
     */
    public static void stage1MapperFailure(int serverId, int tempStage) throws IOException, InterruptedException {
        //1. remove fail server from mapper list
        map_worker_list.remove(Integer.valueOf(serverId));
        if(tempStage == 1)
            System.out.println("Fault occurs during mapping phase");
        else
            System.out.println("Fault occurs during reducing phase");
        System.out.println("Failed Server Type: Mapper");

        //2. check if there is idle server that can be assigned to do the map job
        if(idel_worker_list.size() < 1) {
            System.out.println("Server-Down,no other server available, job failed");
            sendFinishToClient("Server-Down,no other server available, job failed");
            sleep(5000L);
            java.lang.System.exit(1);
        }

        //3. remove idle server from idle list, put new server into mapper list
        //and assign it with a role
        int new_map_server_id = idel_worker_list.get(0);
        idel_worker_list.remove(0);
        map_worker_list.add(new_map_server_id);
        System.out.println("Reassigning mapper role to new Server");
        sendAssignMessage(new_map_server_id, "mapper", job);

        //4. manage variable related to failed server and new server
        int temp_partition_id = serverToPartitionMapTable.get(serverId);
        partitionToServerMapTable.put(temp_partition_id, new_map_server_id);

        serverToPartitionMapTable.put(new_map_server_id, temp_partition_id);
        serverToPartitionMapTable.put(serverId, -1);
        int startLine = partitionStartAndEndLineDict.get(temp_partition_id).getFirst();
        int endLine = partitionStartAndEndLineDict.get(temp_partition_id).getSecond();

        //5. remove failed server's status
        //and set new server status
        mapperPartitionStatus.remove(serverId);
        if(partitionToMapperFilenameMapTable.containsKey(temp_partition_id)) {
            String oldFilePath = partitionToMapperFilenameMapTable.get(temp_partition_id);
            for(int i = 0; i < mapperGenerateResultFilePath.size(); i++) {
                if(mapperGenerateResultFilePath.get(i).equals(oldFilePath)) {
                    mapperGenerateResultFilePath.remove(i);
                    continue;
                }
            }
            mapperPartitionStatus.put(new_map_server_id, false);
            partitionToMapperFilenameMapTable.remove(serverId);
        }

        //6. send start message to the new mapper, the whole job continue to work
        System.out.println("re-send start mapping message to new mapper");
        sendRequestToMapper(new_map_server_id, startLine, endLine, temp_partition_id);
    }

    /**
     * handle reducer's failure during mapping phase
     * @param serverId
     * @throws IOException
     */
    public static void stage1ReducerFailure(int serverId) throws IOException {
        System.out.println("Fault occurs during stage 1, type: reducer");

        //1. remove failed server from reducer list
        reduce_worker_list.remove(Integer.valueOf(serverId));

        //2. check if there is available idle server
        if(idel_worker_list.size() < 1) {
            System.out.println("Server-Down,no other server available, job failed");
            sendFinishToClient("Server-Down,no other server available, job failed");
        }

        //3. put new server into the list
        int new_reduce_server_id = idel_worker_list.get(0);
        idel_worker_list.remove(0);
        reduce_worker_list.add(new_reduce_server_id);

        //4. assign this server with reducer role and hashcode
        int tempHashCode = -1;
        for(int i : reducerHashCode.keySet()) {
            if(reducerHashCode.get(i) == serverId) {
                tempHashCode = i;
                continue;
            }
        }
        reducerHashCode.put(tempHashCode, new_reduce_server_id);
        reducerFinishStatus.remove(serverId);
        reducerFinishStatus.put(new_reduce_server_id, false);
        sendAssignMessage(new_reduce_server_id, "reducer", job);
    }

    /**
     * handle reducing phase's mapper failure
     * since in real world, mapper failure will lead to
     * reducer can get file for mapper server's storage
     * @param serverId failed server's id
     * @throws IOException
     * @throws InterruptedException
     */
    public static void stage2MapperFailure(int serverId) throws IOException, InterruptedException {
        System.out.println("Fault occurs during stage 2, type: mapper");

        //1. set the stage back to mapping phase
        //since mapper generate files that all reducer will need
        //reducing will be abort, and will restart once the new mapper finish its job
        stage = 1;

        //2. same as fault occurs in mapping phase
        stage1MapperFailure(serverId, 1);
    }

    public static void stage2ReducerFailure(int serverId) throws IOException {
        System.out.println("Fault occurs during stage 2, type: reducer");

        //1. remove failed server from reducer list
        reduce_worker_list.remove(Integer.valueOf(serverId));

        //2. check if there is idle server that can be used
        if(idel_worker_list.size() < 1) {
            System.out.println("Server-Down,no other server available, job failed");
            sendFinishToClient("Server-Down,no other server available, job failed");
        }

        //3. manage varibles related to new server and failed server
        int new_reduce_server_id = idel_worker_list.get(0);
        idel_worker_list.remove(0);
        reduce_worker_list.add(new_reduce_server_id);
        reducerFinishStatus.remove(serverId);
        reducerFinishStatus.put(new_reduce_server_id, false);

        int tempHashCode = -1;
        for(int i : reducerHashCode.keySet()) {
            if(reducerHashCode.get(i) == serverId) {
                tempHashCode = i;
                continue;
            }
        }
        reducerHashCode.put(tempHashCode, new_reduce_server_id);

        //4. assign role to the new server
        sendAssignMessage(new_reduce_server_id, "reducer", job);

        //5. tells the new server to start working
        try{
            sendReducerStartMessage(reducerHashCode.get(tempHashCode), -1, filePathForReducers.get(tempHashCode), tempHashCode);
        } catch (IOException e) {
            System.out.println("failed to send message to reducer");
        }

    }

}
