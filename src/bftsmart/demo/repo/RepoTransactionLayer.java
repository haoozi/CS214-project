package bftsmart.demo.repo;




import java.util.*;




class RepoTransactionLayer {

    private HashMap<Integer, RepoTransaction> transactions;
    private int lastXID;

    public RepoTransactionExecutor executor;


    public RepoTransactionLayer() {
        lastXID = 1;

        transactions = new HashMap<Integer, RepoTransaction>();

        this.executor = new RepoTransactionExecutor();
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


    public int read(int tid, Integer key) {

        RepoTransaction trans = this.transactions.get(tid);

        trans.readFromServer(key);

        // TODO
        // trans.cachedWrites Find actionObject.key == key
        // if found, return data from cachedWrites
        //else

        // int data = executor.read(key);
        // return data;

        int value = executor.doRead(key);

        return value;
    }


    public boolean write(int tid, Integer key, Integer value) {
        this.transactions.get(tid).writeToServer(key, value);

        executor.doWrite(key, value);

        return true;
    }


    // Atomic
    public boolean abort(int tid) {
        this.transactions.remove(tid);

        return true;
    }


    // Atomic
    public boolean commit(int tid) {
        RepoTransaction trans = this.transactions.get(tid);

        this.transactions.remove(tid);

        // OCC

        // for everyRead in trans.readHistory
        //     for eachUncommited in transactions:
        //          if (everyRead in eachUncommited.cachedWrites):
        //                return False
        // Call executor to do writes
        // return True


        return true;
    }
}
