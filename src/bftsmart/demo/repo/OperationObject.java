package bftsmart.demo.repo;



import java.util.ArrayList;



class OperationObject {
    // RepoOperationType type;
    int key;
    int value;

    // TODO : Whats this
    // int version;

    public OperationObject(int k, int v) {
        this.key = k;
        this.value = v;

        // this.version = 0;
    }

    public OperationObject(int k) {
        this.key = k;
        this.value = 0;

        // this.version = 0;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}

    