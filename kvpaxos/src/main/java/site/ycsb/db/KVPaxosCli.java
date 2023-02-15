package site.ycsb.db;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * A KV paxos client for YCSB that generates a CSV.
 */
public class KVPaxosCli extends DB {

  private BufferedWriter writer;
  private Random random;
  private final ReentrantLock lock = new ReentrantLock();
  private HashMap<String, Integer> keyMapping;
  private volatile int id;

  @Override
  public void init() throws DBException {
    try {
      writer = new BufferedWriter(new FileWriter("workload.csv"));
      random = new Random();
      keyMapping = new HashMap();
      id = 1;
    } catch (Exception e) {
      System.err.println("Failed to create the file");
    }
  }

  @Override
  public Status read(String arg0, String arg1, Set<String> arg2, Map<String, ByteIterator> arg3) {
    try {
      lock.lock();
      if (!keyMapping.containsKey(arg1)) {
        keyMapping.put(arg1, id++);
      }

      int key = keyMapping.get(arg1);
      writer.write(String.format("%d,%s,%d,", 0, key, 0));
      writer.newLine();
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Status scan(String arg0, String arg1, int arg2, Set<String> arg3,
      Vector<HashMap<String, ByteIterator>> arg4) {
    try {
      lock.lock();
      if (!keyMapping.containsKey(arg1)) {
        keyMapping.put(arg1, id++);
      }
      int key = keyMapping.get(arg1);
      writer.write(String.format("%d,%s,%d,", 2, key, arg2));
      writer.newLine();
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Status update(String arg0, String arg1, Map<String, ByteIterator> arg2) {
    try {
      lock.lock();
      if (!keyMapping.containsKey(arg1)) {
        keyMapping.put(arg1, id++);
      }
      int key = keyMapping.get(arg1);
      writer.write(String.format("%d,%s,%d,", 1, key, 0));
      writer.newLine();
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    try {
      writer.close();
      System.out.print("Last Id:");
      System.out.println(id);
    } catch (Exception e) {
      System.err.println("Failed to close the file");
    }
  }

  @Override
  public Status delete(String arg0, String arg1) {
    return Status.OK;
  }

  @Override
  public Status insert(String arg0, String arg1, Map<String, ByteIterator> values) {
    try {
      lock.lock();
      if (!keyMapping.containsKey(arg1)) {
        keyMapping.put(arg1, id++);
      }
      int key = keyMapping.get(arg1);
      writer.write(String.format("%d,%s,%d,", 1, key, 0));
      writer.newLine();
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    } finally {
      lock.unlock();
    }
  }
}
