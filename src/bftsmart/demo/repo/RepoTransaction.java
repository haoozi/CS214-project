package bftsmart.demo.repo;



import java.util.ArrayList;



class RepoTransaction {

    int transactionID;

    public RepoTransaction(int tid) {
        this.transactionID = tid;
    }


    public OperationObject readFromServer(int key) {
        OperationObject obj = new OperationObject(key);

        return obj;
    }

    public OperationObject writeToServer(int key, int value) {
        OperationObject obj = new OperationObject(key, value);

        return obj;
    }
}
