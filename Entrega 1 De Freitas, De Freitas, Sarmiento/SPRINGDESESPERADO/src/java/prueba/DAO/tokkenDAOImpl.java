/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.DAO;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import java.util.logging.Logger;
import me.prettyprint.cassandra.dao.SimpleCassandraDao;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.springframework.util.DigestUtils;
import prueba.modelo.Tokken;
import prueba.modelo.Usuario;

/**
 *
 * @author Antonio
 */
public class tokkenDAOImpl implements tokkenDAO {
    
        private static final String CF_NAME = "Token";
	private static final String KS_NAME = "Proyecto";
	private static final String HOST_PORT = "localhost:9160";
	
        private static final String CN_USER = "usuario";
	private static final String CN_TOKKEN = "token";
        private static final String CN_DATE = "fecha_creacion";
	private static final String CN_IP = "ip";
        
        private SimpleCassandraDao cassandraDao;
	private Keyspace keyspace;
	
	public tokkenDAOImpl() {
            System.out.println("hola tokkenDAOImpl");
		cassandraDao = new SimpleCassandraDao();
		Cluster cluster = HFactory.getOrCreateCluster("Test-Cluster", HOST_PORT);
		keyspace = HFactory.createKeyspace(KS_NAME, cluster);
		
		cassandraDao.setKeyspace(keyspace);
		cassandraDao.setColumnFamilyName(CF_NAME);
            System.out.println("hola2 tokkenDAOImpl");
	}
        
        @Override
	public boolean insertTokken(Tokken token) {
		String key = token.getUsuario();  //AQUI VA EL VALOR DEL INDEX
                String tok = token.getTokken();
		String date = token.getFecha_creacion();
                String dir = token.getIp();

		
//		Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
		
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_PASSWORD, password, StringSerializer.get(), StringSerializer.get()));
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_CORREO, correo, StringSerializer.get(), StringSerializer.get()));
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_CORREO, twitter, StringSerializer.get(), StringSerializer.get()));
//                mutator.execute();
		
                System.out.println("RowKey: "+key);
                
		cassandraDao.insert(key, CN_USER, key);
		cassandraDao.insert(key, CN_TOKKEN, tok);
                cassandraDao.insert(key, CN_DATE, date);
                cassandraDao.insert(key, CN_IP, dir);

		return true;
	}
        
        @Override
	public Tokken findTokken(String username) {
		//check if row key exist, then query
		CountQuery<String, String> countQuery = HFactory.createCountQuery(keyspace, StringSerializer.get(), StringSerializer.get());
		countQuery.setColumnFamily(CF_NAME);
                System.out.println("Variable: "+username);
		countQuery.setKey(username);
		countQuery.setRange(null, null, 100);
		QueryResult<Integer> countResult = countQuery.execute();
		int count = countResult.get();
		if(count==0) 
		{
                    System.out.println("ENTRO EN EL IF FINDTOKKEN");
                    Tokken tok = new Tokken();
                    
                    tok.setTokken("");
                    
                    return tok;
		} 
		else
		{
                    System.out.println("ENTRO EN EL FINDTOKKEN");
			Tokken tok = new Tokken();
			tok.setUsuario(username);
                        
//                        user.setUsername(username);
//			
//			SliceQuery<String, String, String>  sliceQuery = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
//			sliceQuery.setColumnFamily(CF_NAME);
//			sliceQuery.setRange(null, null, false, 1000);
//			sliceQuery.setKey(username);
//			sliceQuery.setColumnNames(CN_PASSWORD,CN_CORREO,CN_TWITTER);
//			
//			QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
//			ColumnSlice<String, String> columnSlice= result.get();
			
////			logger.debug("columnSlice isNull?="+(columnSlice==null));
////			logger.debug("columnSlice.getColumnByName(CN_PASSWORD) isNull?="+(columnSlice.getColumnByName(CN_PASSWORD)==null)); 
////			HColumn<String, String> columnPassword = columnSlice.getColumnByName(CN_PASSWORD);
////			String md5Password = (columnPassword != null ? columnPassword.getValue() : null);
//			
////			HColumn<String, String> columnDate = columnSlice.getColumnByName(CN_CREATED_AT);
////			String dateStr = (columnDate != null ? columnDate.getValue() : null);
////			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
////			try {
////				Date date = formatter.parse(dateStr);
////				user.setCreated_at(date);
////			} catch (ParseException e) {
////				logger.debug(e.getMessage());
////			}
////			user.setPassword(md5Password);
                        
			String usuario = cassandraDao.get(username, CN_USER);
			tok.setUsuario(usuario);
			String token = cassandraDao.get(username, CN_TOKKEN);
			tok.setTokken(token);                       
			String fecha = cassandraDao.get(username, CN_DATE);
			tok.setFecha_creacion(fecha);
                        String dir = cassandraDao.get(username, CN_IP);
			tok.setIp(dir);
                     
			
//			String dateStr = cassandraDao.get(username, CN_CREATED_AT);
//			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			try {
//				Date date = formatter.parse(dateStr);
//				user.setCreated_at(date);
//			} catch (ParseException e) {
//				logger.debug(e.getMessage());
//			}
			
			return tok;
		}
	}
    
}
