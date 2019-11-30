package bftsmart.demo.repo;

import java.util.*;
import bftsmart.tom.ServiceProxy;

class RepoTransactionLayer {

    private HashMap<Integer, RepoTransaction> transactions;
    private int lastXID;
    private ServiceProxy serviceProxy;
    private ArrayList<OperationObject> readOperations;
    private ArrayList<OperationObject> writeOperations;
    private HashMap<Integer, HashSet<Integer>> readHistory; // key, transac ids
    private HashMap<Integer, HashSet<Integer>> cachedWrites;  // key, transac ids

    // private RepoExecutor executor;


    public RepoTransactionLayer(ServiceProxy serviceProxy) {
        lastXID = 1;
        this.serviceProxy = serviceProxy;
        transactions = new HashMap<Integer, RepoTransaction>();
    }


    public int createTransaction() {
        while(transactions.containsKey(lastXID)) {
            lastXID++;
        }

        int tid = lastXID;
        lastXID ++;

        RepoTransaction trans = new RepoTransaction(tid);
        transactions.put(tid, trans);

        return tid;
    }


    public boolean read(int tid, int key) {

        RepoTransaction trans = transantions.get(tid);

        if(trans != null) {
            OperationObject obj = trans.readFromServer(key);
            readOperations.add(obj);

            HashSet<Integer> rset = readHistory.getOrDefault(key, new HashSet<>());
            rset.add(tid);
            readHistory.put(key, rset);

            return true;
        }
            
        return false;        
    }

    public ArrayList<Integer> getReads() {
        return readOperations;
    }

    public boolean write(int tid, int key, int value) {
        RepoTransaction trans = transantions.get(tid);

        if(trans != null) {
            OperationObject obj = trans.writeToServer(key, value);
            writeOperations.add(obj);

            HashSet<Integer> wset = cachedWrites.getOrDefault(key, new HashSet<>());
            wset.add(tid);
            cachedWrites.put(key, wset);

            return true;
        }
        
        return false;
    }

    public ArrayList<Integer> getWrites() {
        return writeOperations;
    }


    // Atomic
    public boolean abort() {
        readOperations.clear();
        writeOperations.clear();
        return true;
    }

    public boolean isEmpty() {
        return readOperations.isEmpty() && writeOperations.isEmpty();
    }

    // Atomic
    public boolean commit() {
        for(Integer key : cachedWrites.keySet()) {
            if(readHistory.contains(key)) {
                for(Integer wid : cachedWrites.get(key)) {
                    for(Integer rid : readHistory.get(key)) {
                        if(wid != rid) {
                            abort();
                            return false;
                        }
                            
                    }
                }
            }
        }
        return true;

        // OCC

        // for everyRead in trans.readHistory
        //     for eachUncommited in transactions:
        //          if (everyRead in eachUncommited.cachedWrites):
        //                return False
        // Call executor to do writes
        // return True
    }
}
