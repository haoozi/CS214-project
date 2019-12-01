

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


import java.util.Random;
import java.time.Instant;
import java.time.Duration;


public class RepoClient {

    public int clientID;
    RepoClientConnector conn;
    Random rand;


    public int config_transactions = 1000;
    public int config_transaction_size = 3;
    public int config_rw_ratio = 95;
    public int config_print_progress = 2;

    public RepoClient(int id) {
        this.clientID = id;
        this.conn = new RepoClientConnector(clientID);
        this.rand = new Random();
    }

    public void setup() {
        int tid = conn.transStart();

        for (int i = 0; i <= 1000; i++) {
            conn.write(tid, i, 999);
        }

        conn.transCommit(tid);
    }


    public void run() {

        System.out.println("Start simple benchmark");

        System.out.println("Setting up...");
        this.setup();
        System.out.println("Done. Now start benchmark");

        System.out.format("Total transactions: %d\n", config_transactions);
        System.out.format("Transaction size: %d\n", config_transaction_size);
        System.out.format("Transaction read ratio %d%%\n", config_rw_ratio);


        Instant startTime = Instant.now();
        for (int i = 0; i < config_transactions; i++) {
            // generate 1 transaction
            int tid = conn.transStart();
            for (int j = 0; j < config_transaction_size; j++) {
                int k = rand.nextInt(1000);
                int v = rand.nextInt(1000);

                int rw = rand.nextInt(100);

                if (rw > config_rw_ratio) {
                    // generate write
                    conn.write(tid, k, v);
                } else {
                    conn.read(tid, k);
                }
            }

            conn.transCommit(tid);

            if (i % config_print_progress == 0) {
                System.out.format("\r%d/%d", i, config_transactions);
            }
        }
        Instant endTime = Instant.now();


        System.out.println("\nBenchmark finished");

        long ms = Duration.between(startTime, endTime).toMillis();

        System.out.print("Total time: ");
        System.out.print(ms);
        System.out.println(" ms");

    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: demo.repo.RepoClient <client id>");
            System.exit(-1);
        }

        int clientID = Integer.parseInt(args[0]);

        RepoClient c = new RepoClient(clientID);


        c.run();

        System.exit(0);

    //     System.out.println("Getting TID");
    //     int tid = conn.transStart();
    //     System.out.print("TID");
    //     System.out.println(tid);
    //     conn.write(tid, 1, 233);
    //     conn.transCommit(tid);
    //
    //     System.out.println("Transaction 1 complete");
    //
    //     tid = conn.transStart();
    //     conn.write(tid, 1, 333);
    //     int d = conn.read(tid, 1);
    //     System.out.println(d);
    //     conn.transCommit(tid);
    }
}
