package bftsmart.demo.repo;

import java.io.Console;
import java.util.Set;

public class RepoClient {

    public static void main(String[] args) {
        if(args.length < 1) {
			System.out.println("Usage: demo.repo.RepoClientConnector <client id>");
		}
		
        int clientId = Integer.parseInt(args[0]);
        ServiceProxy serviceProxy = new ServiceProxy(clientId);
        RepoTransactionLayer transactionLayer = new RepoTransactionLayer(serviceProxy);

        // create transactions for transactionLayer;
        /*
        transactionLayer.createTransaction();
        transactionLayer.read();
        transactionLayer.write();
        ...
        */

        transactionLayer.commit();
        if(!transactionLayer.isEmpty()) {
            try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {
			
                objOut.writeObject(RepoOperationType.READ);
                for(OperationObject read : transactionLayer.getReads()) {
                    objOut.writeObject(read);
                }
                
                objOut.flush();
                byteOut.flush();
                
                byte[] readReply = serviceProxy.invokeOrdered(byteOut.toByteArray());
                
                // TODO: how to deal with reply
                // if (reply.length == 0)
                //     return null;
                // try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                //         ObjectInput objIn = new ObjectInputStream(byteIn)) {
                //     return (V)objIn.readObject();
                // }
                    
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Exception reads of transactions: " + e.getMessage());
            }
            try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {
			
                objOut.writeObject(RepoOperationType.WRITE);
                for(OperationObject write : transactionLayer.getWrites()) {
                    objOut.writeObject(write);
                }
                
                objOut.flush();
                byteOut.flush();
                
                byte[] writeReply = serviceProxy.invokeOrdered(byteOut.toByteArray());
                
                // TODO: how to deal with reply
                // if (reply.length == 0)
                //     return null;
                // try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                //         ObjectInput objIn = new ObjectInputStream(byteIn)) {
                //     return (V)objIn.readObject();
                // }
                    
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Exception writes of transactions: " + e.getMessage());
            }
        }

    }
}