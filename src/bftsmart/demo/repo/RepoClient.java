

// import repoclientconnector


/*
c = new Connector(parameter)


while true :
    t = starttransaction()
    t.read(k)
    t.write(k,v)
    t.commit()


*/


package bftsmart.demo.repo;


public class RepoClient {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: demo.repo.RepoClient <client id>");
            System.exit(-1);
        }

        int clientID = Integer.parseInt(args[0]);

        RepoClientConnector conn = new RepoClientConnector(clientID);

        System.out.println("Getting TID");
        int tid = conn.transStart();
        System.out.print("TID");
        System.out.println(tid);
        conn.write(tid, 1, 233);
        conn.transCommit(tid);

        System.out.println("Transaction 1 complete");

        tid = conn.transStart();
        conn.write(tid, 1, 333);
        int d = conn.read(tid, 1);
        System.out.println(d);
        conn.transCommit(tid);
    }
}
