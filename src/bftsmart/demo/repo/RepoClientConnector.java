
package bftsmart.demo.map;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import bftsmart.tom.ServiceProxy;

public class RepoClient {
    ServiceProxy serviceProxy;

    public RepoClient(int clientID) {
        serviceProxy = new ServiceProxy(clientID);
    }

    public int transStart() {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {

			objOut.writeObject(RepoOperationType.XSTART);

			objOut.flush();
			byteOut.flush();

			byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
			if (reply.length == 0)
				return null;
			try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
					ObjectInput objIn = new ObjectInputStream(byteIn)) {
				return (int)objIn.readObject();
			}

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception getting transaction ID: " + e.getMessage());
		}
    }


    public boolean transCommit(int tid) {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {

			objOut.writeObject(RepoOperationType.XCOMMIT);
            objOut.writeObject(tid);

			objOut.flush();
			byteOut.flush();

			byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
			if (reply.length == 0)
				return false;
			try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
					ObjectInput objIn = new ObjectInputStream(byteIn)) {
				int stateCode = (int)objIn.readObject();

                if (stateCode == 1) {
                    return true;
                } else {
                    return false;
                }
			}

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception getting transaction ID: " + e.getMessage());
		}
    }

    public void transAbort(int tid) {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {

			objOut.writeObject(RepoOperationType.XABORT);
            objOut.writeObject(tid);

			objOut.flush();
			byteOut.flush();

			serviceProxy.invokeOrdered(byteOut.toByteArray());

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception getting transaction ID: " + e.getMessage());
		}
    }

    public int read(int tid, int key) {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {

			objOut.writeObject(RepoOperationType.READ);
            objOut.writeObject(tid);
            objOut.writeObject(key);

			objOut.flush();
			byteOut.flush();

			byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
			if (reply.length == 0)
				return null;
			try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
					ObjectInput objIn = new ObjectInputStream(byteIn)) {
				return (int)objIn.readObject();
			}

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception getting transaction ID: " + e.getMessage());
		}
    }

    public boolean write(int tid, int key, int value) {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {

			objOut.writeObject(RepoOperationType.WRITE);
            objOut.writeObject(tid);
            objOut.writeObject(key);
            objOut.writeObject(value);

			objOut.flush();
			byteOut.flush();

            serviceProxy.invokeOrdered(byteOut.toByteArray());

            return true;
		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception getting transaction ID: " + e.getMessage());
		}
    }

}
