package org.sunbird.helper;

import java.util.HashMap;
import java.util.Map;

/** Created by arvind on 10/10/17. */
public class CassandraConnectionMngrFactory {

  private static Map<String, CassandraConnectionManager> connectionFactoryMap = new HashMap<>();
  public static final String EMBEDDED_MODE = "embedded";
  public static final String STANDALONE_MODE = "standalone";

  /**
   * Factory method to get the cassandra connection manager oject on basis of mode name pass in
   * argument . default mode is standalone mode .
   *
   * @param name
   * @return
   */
  public static CassandraConnectionManager getObject(String name) {

    if (!connectionFactoryMap.containsKey(name)) {
      // create object
      synchronized (CassandraConnectionMngrFactory.class) {
        if (name.equalsIgnoreCase(EMBEDDED_MODE)) {
          String mode = EMBEDDED_MODE;
          if (null == connectionFactoryMap.get(EMBEDDED_MODE)) {
            CassandraConnectionManager embeddedcassandraConnectionManager =
                new CassandraConnectionManagerImpl(mode);
            connectionFactoryMap.put(EMBEDDED_MODE, embeddedcassandraConnectionManager);
          }
        } else {
          if (null == connectionFactoryMap.get(STANDALONE_MODE)) {
            String mode = STANDALONE_MODE;
            CassandraConnectionManager standaloneCassandraConnectionManager =
                new CassandraConnectionManagerImpl(mode);
            connectionFactoryMap.put(STANDALONE_MODE, standaloneCassandraConnectionManager);
          }
        }
      }
    }
    return connectionFactoryMap.get(name);
  }
}
