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

        if (trans == null) {
            System.out.format("Error, tid %d does not exist\n", tid);
            return 0;
        }

        trans.readFromServer(key);

        // trans.cachedWrites Find actionObject.key == key
        // if found, return data from cachedWrites
        //else

        // int data = executor.read(key);
        // return data;

        for (int i = 0; i < trans.cachedWrites.size(); i++) {

            Integer cw_k = trans.cachedWrites.get(i).key;

            if (cw_k == key) {
                Integer cw_v = trans.cachedWrites.get(i).value;

                return cw_v;
            }

        }

        int value = executor.doRead(key);

        return value;
    }


    public boolean write(int tid, Integer key, Integer value) {
        RepoTransaction trans = this.transactions.get(tid);

        if (trans == null) {
            System.out.format("Error, tid %d does not exist\n", tid);
            return false;
        }
        trans.writeToServer(key, value);

        //executor.doWrite(key, value);

        return true;
    }


    // Atomic
    public boolean abort(int tid) {

        // TODO : does tid exist?
        this.transactions.remove(tid);

        return true;
    }


    // Atomic
    public boolean commit(int tid) {

        boolean canCommit = true;

        RepoTransaction trans = this.transactions.get(tid);

        this.transactions.remove(tid);

        // OCC

        // for everyRead in trans.readHistory
        //     for eachUncommited in transactions:
        //          if (everyRead in eachUncommited.cachedWrites):
        //                return False
        // Call executor to do writes
        // return True


        if (canCommit) {
            for (int i = 0; i < trans.cachedWrites.size(); i++) {
                Integer k = trans.cachedWrites.get(i).key;
                Integer v = trans.cachedWrites.get(i).value;
                executor.doWrite(k, v);
            }
        }


        return true;
    }
}
