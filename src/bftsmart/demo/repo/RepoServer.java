package bftsmart.demo.repo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;

public class RepoServer extends DefaultSingleRecoverable {

    private Map<Integer, Integer> replicaMap;
    private Logger logger;

    public RepoServer(int id) {
		replicaMap = new TreeMap<>();
		logger = Logger.getLogger(RepoServer.class.getName());
		new ServiceReplica(id, this, this); // responsible for delivering requests and return replies
    }
    
    public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: demo.map.RepoServer <server id>");
			System.exit(-1);
		}
		// MapServer is also responsible for launching the processes associated with the application and the BFT-SMaRt replication protocol
		new RepoServer(Integer.parseInt(args[0]));
    }
    
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
		byte[] reply = null;
		boolean hasReply = false;
		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(command);
				ObjectInput objIn = new ObjectInputStream(byteIn);
				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {
			RepoOperationType reqType = (RepoOperationType)objIn.readObject();
			switch (reqType) {
                case WRITE:
                    OperationObject writeOp = (OperationObject)objIn.readObject();
                    while(writeOp != null) {
                        int key = writeOp.getKey();
                        int value = writeOp.getValue();

                        replicaMap.put(key, value);

                        objOut.writeObject(writeOp);
                        hasReply = true;

                        writeOp = (OperationObject)objIn.readObject();
                    }
					break;
                case READ:
                    OperationObject readOp = (OperationObject)objIn.readObject();
                    while(readOp != null) {
                        int key = readOp.getKey();
                        Integer value = replicaMap.get(key);
                        if (value != null) {
                            OperationObject readReply = new OperationObject(key, (int)value);
                            objOut.writeObject(readReply);
                            hasReply = true;
                        }
                        readOp = (OperationObject)objIn.readObject();
                    }					
					break;
			}
			if (hasReply) {
				objOut.flush();
				byteOut.flush();
				reply = byteOut.toByteArray();
			} else {
				reply = new byte[0];
			}

		} catch (IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Ocurred during server operation execution", e);
		}
		return reply;
    }
    
    @Override
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

	@SuppressWarnings("unchecked")
	@Override
	public void installSnapshot(byte[] state) {
		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
				ObjectInput objIn = new ObjectInputStream(byteIn)) {
			replicaMap = (Map<Integer, Integer>)objIn.readObject();
		} catch (IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Error while installing snapshot", e);
		}
	}
}