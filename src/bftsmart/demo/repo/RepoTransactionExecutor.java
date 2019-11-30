package bftsmart.demo.repo;



import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;



class RepoTransactionExecutor {

    private Map<int, int> replicaMap;
    private Logger logger;

    public RepoTransactionExecutor() {
        replicaMap = new TreeMap<>();
        logger = Logger.getLogger(RepoTransactionExecutor.class.getName());
    }

    public int doRead(int key) {
        return replicaMap.get(key);
    }

    public boolean doWrite(int key, int value) {
        replicaMap.put(key, value);
    }

    public byte[] getSnapshot() {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
			objOut.writeObject(replicaMap);
			return byteOut.toByteArray();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while taking snapshot", e);
		}
		return new byte[0];
    }


    public void installSnapshot(byte[] state) {
		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
				ObjectInput objIn = new ObjectInputStream(byteIn)) {
			replicaMap = (Map<K, V>)objIn.readObject();
		} catch (IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Error while installing snapshot", e);
		}
	}

}
