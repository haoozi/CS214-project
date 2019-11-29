package bftsmart.demo.repo;



import java.util.ArrayList;



class RepoTransaction {

    int transactionID;

    class actionObject {
        // RepoOperationType type;
        int key;
        int value;

        // TODO : Whats this
        int version;

        public actionObject(int k, int v) {
            this.key = k;
            this.value = v;

            this.version = 0;
        }

        public actionObject(int k) {
            this.key = k;
            this.value = 0;

            this.version = 0;
        }
    }

    public RepoTransaction(int tid) {
        this.transactionID = tid;
    }


    ArrayList<actionObject> readHistory;
    ArrayList<actionObject> cachedWrites;


    public void readFromServer(int key) {
        actionObject obj = new actionObject(key);

        readHistory.add(obj);
    }

    public void writeToServer(int key, int value) {
        actionObject obj = new actionObject(key, value);

        cachedWrites.add(obj);
    }
}
