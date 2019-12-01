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

    private RepoTransactionLayer transLayer;
    private Logger logger;


    public RepoServer(int id) {
        // replicaMap = new TreeMap<>();
        this.transLayer = new RepoTransactionLayer();
		logger = Logger.getLogger(RepoServer.class.getName());
		new ServiceReplica(id, this, this);
    }
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: demo.repo.RepoServer <serverid>");

            System.exit(-1);
        }

        new RepoServer(Integer.parseInt(args[0]));
    }




    	@SuppressWarnings("unchecked")
    	@Override
    	public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
    		byte[] reply = null;
    		int key;
    		int value;
            int tid = 0;
    		boolean hasReply = false;

    		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(command);
    				ObjectInput objIn = new ObjectInputStream(byteIn);
    				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {
    			RepoOperationType reqType = (RepoOperationType)objIn.readObject();
    			switch (reqType) {

                    case READ:
                        tid = (int)objIn.readObject();
                        key = (int)objIn.readObject();

                        value = transLayer.read(tid, key);

                        hasReply = true;
                        objOut.writeObject(value);

                        // System.out.format("READ, tid: %d, key: %d, result value: %d\n", tid, key, value);


                        break;

                    case WRITE:
                        tid = (int)objIn.readObject();
                        key = (int)objIn.readObject();
                        value = (int)objIn.readObject();

                        // System.out.format("WRITE, tid %d, k: %d, v: %d\n", tid, key, value);

                        transLayer.write(tid, key, value);
                        // System.out.println("Complete");
                        break;

                    case XSTART:
                        tid = transLayer.createTransaction();
                        objOut.writeObject(tid);
                        hasReply = true;

                        // System.out.print("XSTART, tid: ");
                        // System.out.println(tid);
                        break;

                    case XCOMMIT:
                        tid = (int)objIn.readObject();
                        hasReply = true;

                        if (transLayer.commit(tid)) {
                            objOut.writeObject(1);
                        } else {
                            objOut.writeObject(0);
                        }

                        // System.out.print("XCOMMIT, tid: ");
                        // System.out.println(tid);
                        break;

                    case XABORT:
                        tid = (int)objIn.readObject();

                        transLayer.abort(tid);

                        // System.out.print("XABORT, tid: ");
                        // System.out.println(tid);
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
    			logger.log(Level.SEVERE, "Ocurred during operation execution", e);
    		}
    		return reply;
    	}












        // TODO: Fix

    	@SuppressWarnings("unchecked")
    	@Override
    	public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
    		byte[] reply = null;
    		int key, value;
    		boolean hasReply = false;

    		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(command);
    				ObjectInput objIn = new ObjectInputStream(byteIn);
    				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {
    			RepoOperationType reqType = (RepoOperationType)objIn.readObject();
    			switch (reqType) {
    				default:
    					logger.log(Level.WARNING, "in appExecuteUnordered only read operations are supported");
    			}
    			if (hasReply) {
    				objOut.flush();
    				byteOut.flush();
    				reply = byteOut.toByteArray();
    			} else {
    				reply = new byte[0];
    			}
    		} catch (IOException | ClassNotFoundException e) {
    			logger.log(Level.SEVERE, "Ocurred during map operation execution", e);
    		}

    		return reply;
    	}


    	@Override
    	public byte[] getSnapshot() {
    		return this.transLayer.executor.getSnapshot();
    	}

    	@SuppressWarnings("unchecked")
    	@Override
    	public void installSnapshot(byte[] state) {
    		this.transLayer.executor.installSnapshot(state);
    	}
}
