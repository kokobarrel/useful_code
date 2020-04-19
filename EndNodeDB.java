package org.net298.database;


import org.net298.TestHarness;
import org.net298.message.Message;
import org.net298.message.TextMessageBean;
import org.net298.utils.DEBUG;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author sqlitetutorial.net
 */
public class EndNodeDB extends NodeSuper {


    private static boolean doBulkInsert = false;
    private static boolean bulkInsertPaused = false;
    public static boolean bulkRunning = false;
    private static Object bulkLock = "lock";

    private static Connection conn = null;
    private ConcurrentLinkedDeque bulkInsertQueue = new ConcurrentLinkedDeque();


    private static String MESSAGE_TABLE = "message_table";
    private static String MESSAGE_ID = "message_id";
    private static String SENDER_ID = "sender_id";
    private static String RECEIVER_ID = "receiver_id";
    private static String STATUS = "status";
    private static String BLOBBY = "blobby";


    private static String messageTableString = MESSAGE_TABLE + "(" + MESSAGE_ID + "," + SENDER_ID + "," + RECEIVER_ID + "," + STATUS + "," + BLOBBY + ")";
    private static String url = "jdbc:sqlite:c:/projects/ssl_tests/" + TestHarness.nodeChosen + "end_node.db";
    private static EndNodeDB thisInstance;

    public EndNodeDB() {
        if (doBulkInsert) {
            startBulkInsert();
        }
    }


    public static synchronized EndNodeDB getInstance() {
        if (thisInstance == null) {
            thisInstance = new EndNodeDB();
            thisInstance.connect();
            thisInstance.dropTables();//TODO remove
            thisInstance.createTables();
        }
        return thisInstance;
    }


    private void startBulkInsert() {

        Thread b = new Thread() {

            public void run() {
                bulkRunning = true;
                while (bulkRunning) {

                    if (!bulkInsertQueue.isEmpty()) {
                        doBulkInsert();
                        bulkInsertPaused = true;
                    }

                    try {
                        if (bulkInsertPaused) {
                            synchronized (bulkLock) {
                                bulkLock.wait();
                            }
                            bulkInsertPaused = false;
                        }

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }// while

            }// run

        };
        b.start();
    }


    /**
     * Connect to a sample database
     */
    private void connect() {

        try {
            // db parameters
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            DEBUG.v(130, "Connection to SQLite has been established.");

        } catch (SQLException e) {
            DEBUG.v(130, e.getMessage());
        }
    }

    public synchronized void closeConnection() {

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    private void createTables() {


        String mt = "CREATE TABLE IF NOT EXISTS " + MESSAGE_TABLE + "("
                + MESSAGE_ID + " text NOT NULL,"
                + SENDER_ID + " text NOT NULL,"
                + RECEIVER_ID + " text NOT NULL,"
                + STATUS + " integer NOT NULL,"
                + BLOBBY + " blob"
                + ");";


        try {

            //conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute(mt);

        } catch (SQLException e) {
            DEBUG.v(130, e.getMessage());
        }

    }


    private void dropTables() {

        String sqlStr2 = "DROP TABLE IF EXISTS " + MESSAGE_TABLE;

        try {

            //conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute(sqlStr2);

        } catch (SQLException e) {
            DEBUG.v(130, e.getMessage());
        }

    }


    private synchronized void testMessageTablePrint() {

        String sql = "SELECT * FROM " + MESSAGE_TABLE;

        try {

            //conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {

                DEBUG.v(130, rs.getString(MESSAGE_ID));
                DEBUG.v(130, rs.getString(SENDER_ID));
                DEBUG.v(130, rs.getString(RECEIVER_ID));
                //DEBUG.v(130, rs.getString(STATUS));

                byte[] t = rs.getBytes(BLOBBY);
                Message t44 = (Message) byteToObj(t);
                DEBUG.v(130, "send guid=" + t44.getSenderGuid());
                DEBUG.v(130, "rec guid=" + t44.getRecipientGuid());
                DEBUG.v(130, "message id=" + t44.getMessageId());

                DEBUG.v(130, "-----------------");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }


    public synchronized void doBulkInsert() {


        int count = 0;
        Message mo56 = null;
        Connection conn = null;
        PreparedStatement psBulk = null;

        try {
            conn = DriverManager.getConnection(url);
            conn.setAutoCommit(false);
            psBulk = conn.prepareStatement("insert into " + messageTableString + " values(?,?,?,?,?);");
            DEBUG.v(1000, "123777 doiBultInsert..");

            while (!bulkInsertQueue.isEmpty() && count < 100) {

                mo56 = (Message) bulkInsertQueue.poll();
                DEBUG.v(700, "new message bulk insert..");

                if(mo56 != null) {

                    try {

                        System.out.print(".");
                        //DEBUG.v(1000, "123777 addMessage..");

                        psBulk.setString(1, mo56.getMessageId());
                        psBulk.setString(2, mo56.getSenderGuid());
                        psBulk.setString(3, mo56.getRecipientGuid());
                        psBulk.setInt(4, mo56.getStatus());
                        psBulk.setBytes(5, objToByte(mo56));
                        psBulk.addBatch();
                        //DEBUG.v(1000, "123777 data added to batch..");

                    } catch (SQLException e) {
                        DEBUG.v(130, "error thrown..");
                        e.printStackTrace();
                        bulkInsertQueue.add(mo56);
                    } catch (IOException e) {
                        e.printStackTrace();
                        bulkInsertQueue.add(mo56);
                    }

                }

            }//while

            try {
                int[] updateCounts = psBulk.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);
                DEBUG.v(700, "bulk inserts=" + updateCounts);
                synchronized (bulkLock) {
                    bulkLock.notify();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    conn.rollback();
                    conn.setAutoCommit(true);
                    synchronized (bulkLock) {
                        bulkLock.notify();
                    }
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    synchronized (bulkLock) {
                        bulkLock.notify();
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            synchronized (bulkLock) {
                bulkLock.notify();
            }
        }

    }


    public synchronized void addMessage(Message mo56) {

        if (doBulkInsert) {
            bulkInsertQueue.add(mo56);
        } else {
            try {

                DEBUG.v(1000, "123777 addMessage..");
                PreparedStatement messStatement = conn.prepareStatement("insert into " + messageTableString + " values(?,?,?,?,?);");
                messStatement.setString(1, mo56.getMessageId());
                messStatement.setString(2, mo56.getSenderGuid());
                messStatement.setString(3, mo56.getRecipientGuid());
                messStatement.setInt(4, mo56.getStatus());
                messStatement.setBytes(5, objToByte(mo56));
                messStatement.executeUpdate();
                DEBUG.v(1000, "123777 data added..");

            } catch (SQLException e) {
                DEBUG.v(130, "error thrown..");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /*
     * An end client picks up any waiting messages.
     */
    public synchronized ArrayList getMessages(String receiverId) {

        ArrayList messagesArray = new ArrayList();
        String sql = "SELECT * FROM " + MESSAGE_TABLE + " WHERE " + RECEIVER_ID + "='" + receiverId + "';";

        try {

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                DEBUG.v(130, rs.getString(MESSAGE_ID));
                DEBUG.v(130, rs.getString(SENDER_ID));
                DEBUG.v(130, rs.getString(RECEIVER_ID));
                DEBUG.v(130, rs.getString(STATUS));
                byte[] t = rs.getBytes(BLOBBY);
                //String t44 = (String) byteToObj(t);
                //DEBUG.v(130, "t44=" + t44.toString());

                messagesArray.add(byteToObj(t));
                DEBUG.v(130, "-----------------");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            return messagesArray;
        }

    }



    public synchronized int getMessageStatus(Message m) {

        String sql = "SELECT * FROM " + MESSAGE_TABLE + " WHERE " + MESSAGE_ID + "='" + m.getMessageId() + "'";
        int status = -1;

        try {

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                status = rs.getInt(STATUS);
                DEBUG.v(130, "-----------------");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            return status;
        }
    }


    public synchronized boolean hasMessages(String receiverId) {

        boolean has = false;
        String sql = "SELECT * FROM " + MESSAGE_TABLE + " WHERE " + RECEIVER_ID + "='" + receiverId + "';";

        try {

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                has = true;
                DEBUG.v(130, "-----------------");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            return has;
        }
    }


    public synchronized boolean hasMessage(Message m26) {

        boolean has = false;
        String messageId = m26.getMessageId();
        String sql = "select * from " + MESSAGE_TABLE + " WHERE " + MESSAGE_ID + "='" + messageId + "';";

        try {

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                has = true;
                DEBUG.v(130, "-----------------");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            return has;
        }
    }


    public synchronized void deleteMessage(Message m) {

        String messageId = m.getMessageId();
        String sql = "DELETE FROM " + MESSAGE_TABLE + " WHERE " + MESSAGE_ID + "='" + messageId + "';";

        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public synchronized void updateMessageStatus(Message m, int status) {
        String messageId = m.getMessageId();
        String sql = "update " + MESSAGE_TABLE + " set " + STATUS + "='" + status +
                "' where " + MESSAGE_ID + "='" + messageId + "'";

        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static byte[] objToByte(Object obj33) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
        objStream.writeObject(obj33);

        return byteStream.toByteArray();
    }

    public static Object byteToObj(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objStream = new ObjectInputStream(byteStream);
        return objStream.readObject();
    }


    public static String getDatabaseUrl() {
        return url;
    }

    public static void setUseBulkInsert(boolean u) {
        doBulkInsert = u;
    }

    public void setBulkInsertPaused(boolean bulkInsertPaused) {
        this.bulkInsertPaused = bulkInsertPaused;
    }


    public void setBulkInsertUnpaused(boolean bulkInsertPaused) {
        this.bulkInsertPaused = bulkInsertPaused;
    }

    public void setBulkRunning(boolean bulkRunning) {
        this.bulkRunning = bulkRunning;
    }

    private static void test() {

    }


    public static void main(String[] args) {

        Message m1 = new Message("send222", "rec888");
        Message m2 = new Message("send223", "rec881");
        Message m3 = new Message("send224", "rec882");
        Message m4 = new Message("send225", "rec883");

        EndNodeDB.setUseBulkInsert(true);
        EndNodeDB endb = EndNodeDB.getInstance();


/*
//        endb.addMessage("messId112255", "sendID1", "recID1", blobStr);
        endb.addMessage(m1);
        DEBUG.v(1000, "m1 msg status=" + endb.getMessageStatus(m1));
        DEBUG.v(1000, "updating message status..");

        endb.updateMessageStatus(m1, STATUS_MESSAGE_RECEIVED);
        DEBUG.v(1000, "m1 msg status=" + endb.getMessageStatus(m1));


        DEBUG.v(1000, "-------------------------");
        //endb.addMessage("messId314255", "sendID2", "recID1", blobStr);
        endb.addMessage(m2);

        DEBUG.v(1000, "m2 msg status=" + endb.getMessageStatus(m2));
        DEBUG.v(1000, "updating message status..");

        endb.updateMessageStatus(m2, STATUS_MESSAGE_RECEIVED);
        DEBUG.v(1000, "m2 msg status=" + endb.getMessageStatus(m2));


        DEBUG.v(1000, "-------------------------");
        //endb.addMessage("messId6672599", "sendID3", "recID2", blobStr);
        endb.addMessage(m3);

        DEBUG.v(1000, "m3 msg status=" + endb.getMessageStatus(m3));
        DEBUG.v(1000, "updating message status..");

        endb.updateMessageStatus(m3, STATUS_MESSAGE_RECEIVED);
        DEBUG.v(1000, "m3 msg status=" + endb.getMessageStatus(m3));


        DEBUG.v(1000, "-------------------------");
        //endb.addMessage("messId667255", "sendID3", "recID3", blobStr);
        endb.addMessage(m3);


        DEBUG.v(1000, "-------------------------");
        //endb.addMessage("messId99978", "sendID3", "recID4", blobStr);
        endb.addMessage(m4);

        DEBUG.v(1000, "-------------------------");
        DEBUG.v(1000, "m4 msg status=" + endb.getMessageStatus(m1));
        DEBUG.v(1000, "updating message status..");

        endb.updateMessageStatus(m4, STATUS_MESSAGE_RECEIVED);
        DEBUG.v(1000, "m4 msg status=" + endb.getMessageStatus(m4));


        //   endb.addMessage("stats2");
        // endb.addMessage("stats3");

        //endb.testMessageTablePrint();

        //endb.getMessages("recID1");
        endb.getMessages(m1.getRecipientGuid());

        DEBUG.v(1000, "-------------------------");
        //endb.getMessages("recID3");
        endb.getMessages(m2.getRecipientGuid());

        DEBUG.v(1000, "-------------------------");
        //endb.getMessages("recID4");
        endb.getMessages(m3.getRecipientGuid());

        DEBUG.v(1000, "-------------------------");
        //endb.deleteMessage("messId99978");
        endb.deleteMessage(m4);

        DEBUG.v(1000, "-------------------------");
        DEBUG.v(1000, "m4 msg status=" + endb.getMessageStatus(m4));
        DEBUG.v(1000, "updating message status..");

        endb.updateMessageStatus(m4, STATUS_MESSAGE_RECEIVED);
        DEBUG.v(1000, "m4 msg status=" + endb.getMessageStatus(m4));


        DEBUG.v(1000, "----------print---------------");
//        endb.testMessageTablePrint();

*/

        int i = 0;

        for (i = 0; i < 10000; i++) {
            TextMessageBean t = new TextMessageBean("sendy", "receivy", UUID.randomUUID().toString());
            endb.addMessage(t);
        }
        DEBUG.v(1000, "added " + i + " messages..");
        DEBUG.v(1000, "-------------------------");


        endb.closeConnection();
    }


}