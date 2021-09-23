import java.util.*;

public class Config {
    /**
     * config file
     * define number of servers in the system, and their ip port
     * users can define number of map worker, reduce worker here
     */
    public static int client_port = 8080;
    public static int master_port = 34000;
    public static int num_wokers = 6;
    public static int num_mappers = 2;
    public static int num_reducer = 3;
    public final static int Master = 1; //Master's role is indicated by 1
    public final static int Worker = 2; //Worker's role is indicated by 2
    public final static int mapper = 1; //Worker as mapper is indicated by 1
    public final static int reducer = 2; //Worker as reducer is indicated by 2
    public final static int notAssigned = 0;
    public static HashMap<Integer, Integer> worker_port;
    public Config() {
        /**
         * init a hashmap to store worker server's ip address
         */
        worker_port = new HashMap<Integer, Integer>();
        for(int i = 0; i < num_wokers; i++) {
            worker_port.put(i+1, master_port+i+1);
        }
    }

    /**
     * return master server's port number
     * @return type: int, master server's port number
     */
    public int getMasterPort() {
        return master_port;
    }

    /**
     * return all worker server's port number in a hashmap
     * @return type: hashmap, all worker server's port number
     */
    public HashMap getWokerPort() {
        return worker_port;
    }

    /**
     * return worker's port for specific worker
     * @param id type: int, id of the worker server
     * @return type:int, woker server's port number
     */
    public int getWorkertPort(int id) {
        return worker_port.get(id);
    }

    //return worker numbers
    public int getNumWorkers() {
        return num_wokers;
    }

    //return reducer numbers
    public int getNumReducer() {
        return num_reducer;
    }

    //return mapper numbers
    public int getNumMapper() {
        return num_mappers;
    }

    //return client's port number
    public int getClientPort() {return client_port;}

    //When testing fault tolerance, this variable is used to indicate
    //which server we plan to shut down
    //and at which stage the server become unavailable
    //set to -1 if you don't want any failure
    public final static int failWorkerId =-1;
    public final static int failStage = -1;

}
