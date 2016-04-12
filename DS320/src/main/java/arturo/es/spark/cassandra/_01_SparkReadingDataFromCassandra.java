package arturo.es.spark.cassandra;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.datastax.spark.connector.rdd.ReadConf;

public class _01_SparkReadingDataFromCassandra {

	// root@sandbox.hortonworks.com:/root-# apache-cassandra-2.1.13/bin/cassandra
	// root@sandbox.hortonworks.com:/root-# apache-cassandra-2.1.13/bin/cqlsh
	
	public static void main(String[] args) {
  	  	Logger.getLogger("org").setLevel(Level.OFF);
  	  	Logger.getLogger("akka").setLevel(Level.OFF);
  	  	
		SparkConf sparkConf = new SparkConf()
				.setMaster("local[4]") // spark:///sandbox.hortonworks.com:7077
				.setAppName("Reading Data From Cassandra")
				.set("spark.cassandra.connection.host", "localhost");
		
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        CassandraTableScanJavaRDD<CassandraRow> tabla = CassandraJavaUtil.javaFunctions(sc).cassandraTable("killrvideo", "videos");
        tabla.cache().setName("TABLA DE CASSANDRA CACHEADA");
  	  	ReadConf configuracion = tabla.rdd().readConf();
  	  	
  	  	System.out.println("Tamaño: "+configuracion.fetchSizeInRows() +
  	  					   ", prefijo "+configuracion.productPrefix());

  	  	CassandraTableScanJavaRDD<CassandraRow> consulta = 
  	  			tabla.select("added_date", "description", "location")
  	  				 .where("description > 'An' AND location > 'http' ");
  	  	
  	  	List<CassandraRow> filas = consulta.collect();
  	  	
  	  	for (CassandraRow fila : filas) {
  	  		System.out.println(fila.toString());
  	  	}
  	  	
	}

}
