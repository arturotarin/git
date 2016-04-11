package arturo.es.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Ejemplo 
{
    public static void main( String[] args )
    {
    	// root@sandbox.hortonworks.com:/root-# apache-cassandra-2.1.13/bin/cassandra
    	// root@sandbox.hortonworks.com:/root-# apache-cassandra-2.1.13/bin/cqlsh
    	Cluster cluster;
    	Session session;
    	
    	// Connect to the cluster and the Keyspace twitter
    	//cluster = Cluster.builder().addContactPoint("sandbox.hortonworks.com").build();
    	cluster = Cluster.builder().addContactPoint("localhost").build();
    	session = cluster.connect("twitter");
    	
    	// Insert one record into the users3 table
    	session.execute("insert into users3 (user_id, email, fullname, password, followers) " +
    					"values ('b', 'b@bb.es', 'barlovento', 'lopez',{'bbbbb':'bbbbbbb'})");
    	
    	// Use select to get the user we just entered
    	ResultSet results = session.execute("select * from users3 where user_id='b'");
    	
    	for (Row row:results) {
    		System.out.format("%s %s %s\n", row.getString("user_id"), row.getString("fullname"), row.getString("email"));
    	}
    	
    	session.close();
    	cluster.close();
    }
}
