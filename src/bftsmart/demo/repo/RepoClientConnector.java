
package bftsmart.demo.repo;

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

public class RepoClientConnector {
    ServiceProxy serviceProxy;

    public RepoClientConnector(int clientID) {
        serviceProxy = new ServiceProxy(clientID);
    }

    public int transStart() {
        int tid = 0;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {

			objOut.writeObject(RepoOperationType.XSTART);

			objOut.flush();
			byteOut.flush();

			byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
			if (reply.length == 0)
				return 0;
			try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
					ObjectInput objIn = new ObjectInputStream(byteIn)) {
				tid = (int)objIn.readObject();
            return tid;
			}

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception when XSTART: " + e.getMessage());
		}
        return tid;
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
			System.out.println("Exception when XCOMMIT: " + e.getMessage());
		}

        return false;
    }

    public void transAbort(int tid) {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {

			objOut.writeObject(RepoOperationType.XABORT);
            objOut.writeObject(tid);

			objOut.flush();
			byteOut.flush();

			serviceProxy.invokeOrdered(byteOut.toByteArray());

		} catch (IOException e) {
			System.out.println("Exception when XABORT: " + e.getMessage());
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
				return 0;
			try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
					ObjectInput objIn = new ObjectInputStream(byteIn)) {
				return (int)objIn.readObject();
			}

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Exception when READ: " + e.getMessage());
		}

        return 0;
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

		} catch (IOException e) {
			System.out.println("Exception when WRITE: " + e.getMessage());
		}

        return true;
    }

}
