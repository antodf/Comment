/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.DAO;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import java.util.logging.Logger;
import me.prettyprint.cassandra.dao.SimpleCassandraDao;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.springframework.util.DigestUtils;
import prueba.modelo.Comentario;
import prueba.modelo.Usuario;

import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.exceptions.HectorException;

import me.prettyprint.hector.api.mutation.MutationResult;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import prueba.modelo.Etiqueta;

/**
 *
 * @author Antonio
 */
public class comentarioDAOImpl implements comentarioDAO {
    
	private static final String CF_NAME = "Comentario";
        private static final String CF_NAME_TAG = "Comentario_Etiqueta";
	private static final String KS_NAME = "Proyecto";
	private static final String HOST_PORT = "localhost:9160";
	
        private static final String CN_ID = "id";
	private static final String CN_CONTENT = "contenido";
        private static final String CN_DATE = "fecha_creacion";
	private static final String CN_ADJUNTO = "adjunto";
        private static final String CN_NICK = "nickname";
        private static final String CN_REPLY = "reply";
        
        private String codigo;
        private int codigoINT;
        
        private SimpleCassandraDao cassandraDao;
	private Keyspace keyspace;
        
        private StringSerializer serializer;
        
        private String [] comandos;
        private String [] comandos2;
        private String [] definitivo;
        private String [] arregloFinal = new String [100];
        private String linea; 
        private String res;
        private String res2;
        private int e = 0;
        private String ultimoSplit;
        private int cuenta;
        
        public Comentario instancia(){
            Comentario obj = new Comentario();
            return obj;
        }
        
        public Etiqueta instanciaEtiqueta(){
            Etiqueta obj = new Etiqueta();
            return obj;
        }
        
        public comentarioDAOImpl() {
            System.out.println("hola ComentarioDAOImpl");
		cassandraDao = new SimpleCassandraDao();
		Cluster cluster = HFactory.getOrCreateCluster("Test-Cluster", HOST_PORT);
		keyspace = HFactory.createKeyspace(KS_NAME, cluster);
		
		cassandraDao.setKeyspace(keyspace);
		cassandraDao.setColumnFamilyName(CF_NAME);
            System.out.println("hola2 ComentarioDAOImpl");
	}
        
        @Override
	public boolean insertComentario(Comentario comentario) {
		String key = comentario.getId();  //AQUI VA EL VALOR DEL INDEX
//                String key = "3";
                //String identificador = comentario.getId();
                String contenido = comentario.getmensaje();
		String fecha = comentario.getFecha_creacion();
                String adjunto = comentario.getAdjunto();
                String nickName = comentario.getnickName();
                String re = comentario.getReply();

		
//		Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
		
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_PASSWORD, password, StringSerializer.get(), StringSerializer.get()));
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_CORREO, correo, StringSerializer.get(), StringSerializer.get()));
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_CORREO, twitter, StringSerializer.get(), StringSerializer.get()));
//                mutator.execute();
		
                System.out.println("DAOInsertar: "+contenido);
                System.out.println("DAOInsertar: "+fecha);
                System.out.println("DAOInsertar: "+adjunto);
                System.out.println("DAOInsertar: "+re);
                
		cassandraDao.insert(key, CN_ID, key);
                cassandraDao.insert(key, CN_CONTENT, contenido);
		cassandraDao.insert(key, CN_DATE, fecha);
		cassandraDao.insert(key, CN_ADJUNTO, adjunto);
                cassandraDao.insert(key, CN_NICK, nickName);
                cassandraDao.insert(key, CN_REPLY, re);
		return true;
	}
        
        @Override
        public boolean deleteComentario(String id) {
		//seems this is not an easy API to delete one single row
		//cassandraDao.delete(columnName, keys)
		//
		//try another way
		ColumnFamilyTemplate<String, String> template =
                new ThriftColumnFamilyTemplate<String, String>(keyspace, CF_NAME, StringSerializer.get(), StringSerializer.get());
		//String identificador = String.valueOf(id);
                template.deleteRow(id);
		return true;
	}
                
        @Override
	public Comentario findComentario(String id) {
		//check if row key exist, then query
		CountQuery<String, String> countQuery = HFactory.createCountQuery(keyspace, StringSerializer.get(), StringSerializer.get());
		countQuery.setColumnFamily(CF_NAME);
                System.out.println("Variable: "+id);
                //String identificador = String.valueOf(id);
		countQuery.setKey(id);
		countQuery.setRange(null, null, 100);
		QueryResult<Integer> countResult = countQuery.execute();
		int count = countResult.get();
		if(count==0) 
		{
                    System.out.println("ENTRO EN EL IF FINDCOMENTARIO");
                    Comentario com = new Comentario();
                    
                    com.setId("");
                    return com;
		} 
		else
		{
                    System.out.println("ENTRO EN EL FINDCOMENTARIO");
			Comentario comment = new Comentario();
//			comment.setId(id);
                        
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
                        
			String identificador = cassandraDao.get(id, CN_ID);
			comment.setId(identificador);
			String contenido = cassandraDao.get(id, CN_CONTENT);
			comment.setmensaje(contenido); 
                        System.out.println(contenido);
			String fecha = cassandraDao.get(id, CN_DATE);
			comment.setFecha_creacion(fecha);
                        String adjunto = cassandraDao.get(id, CN_ADJUNTO);
			comment.setAdjunto(adjunto);
                        String nickName = cassandraDao.get(id, CN_NICK);
			comment.setnickName(nickName);
                        String re = cassandraDao.get(id, CN_REPLY);
			comment.setReply(re);
                      
			
//			String dateStr = cassandraDao.get(username, CN_CREATED_AT);
//			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			try {
//				Date date = formatter.parse(dateStr);
//				user.setCreated_at(date);
//			} catch (ParseException e) {
//				logger.debug(e.getMessage());
//			}
			
			return comment;
		}
	}
        
        @Override
        public boolean updateComentario(Comentario comment) {
////		String key = user.getName();
////
////		String password = user.getPassword();
////		String correo = user.getCorreo();
////                String twitter = user.getTwitter();
		
//		Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
//		
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_PASSWORD, md5Password, StringSerializer.get(), StringSerializer.get()));
//		mutator.addInsertion(key, CF_NAME, HFactory.createColumn(CN_CREATED_AT, dateFormatted, StringSerializer.get(), StringSerializer.get()));
//		mutator.execute();
		
////		cassandraDao.insert(key, CN_PASSWORD, password);
////		cassandraDao.insert(key, CN_CORREO, correo);
////                cassandraDao.insert(key, CN_TWITTER, twitter);
		return true;
	}
        
        @Override
	public Collection<Comentario> listComentario() {
		List<Comentario> comentarioList = new ArrayList<Comentario>();
		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		rangeSlicesQuery.setColumnFamily(CF_NAME);
		rangeSlicesQuery.setColumnNames(CN_ID,CN_CONTENT,CN_DATE,CN_ADJUNTO,CN_REPLY);
		rangeSlicesQuery.setRowCount(1000);
//		rangeSlicesQuery.setRange(start, finish, reversed, count);
		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		OrderedRows<String, String, String> rows = result.get();
		Iterator<Row<String, String, String>> itr = rows.iterator();
		while(itr.hasNext()) {
			Row<String, String, String> row = itr.next();
			ColumnSlice<String, String> columnSlice = row.getColumnSlice();
			
			Comentario comment = new Comentario();
			
			String identificador = row.getKey();
			comment.setId(identificador);
                        
                        
                        String contenido = columnSlice.getColumnByName(CN_CONTENT).getValue();
			comment.setmensaje(contenido);
                        
                        String fecha = columnSlice.getColumnByName(CN_DATE).getValue();
			comment.setFecha_creacion(fecha);
			
			String adjunto = columnSlice.getColumnByName(CN_ADJUNTO).getValue();
			comment.setAdjunto(adjunto);
                        
                        String re = columnSlice.getColumnByName(CN_REPLY).getValue();
			comment.setReply(re);
			
//			String dateStr = columnSlice.getColumnByName(CN_CREATED_AT).getValue();
//			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			try {
//				Date date = formatter.parse(dateStr);
//				user.setCreated_at(date);
//			} catch (ParseException e) {
//				logger.debug(e.getMessage());
//			}
			
			comentarioList.add(comment);
                        
		}
		
		return comentarioList;
	}
        
        public String getID(){
            
            List<Comentario> comentarioList = new ArrayList<Comentario>();
            System.out.println("Entre a getID");
		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		rangeSlicesQuery.setColumnFamily(CF_NAME);
		rangeSlicesQuery.setColumnNames(CN_ID,CN_CONTENT,CN_DATE,CN_ADJUNTO);
		rangeSlicesQuery.setRowCount(1000);
//		rangeSlicesQuery.setRange(start, finish, reversed, count);
		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		OrderedRows<String, String, String> rows = result.get();
		Iterator<Row<String, String, String>> itr = rows.iterator();
                int i = 0;
                codigoINT = 0;
                
		while((itr.hasNext())) {
			Row<String, String, String> row = itr.next();
			ColumnSlice<String, String> columnSlice = row.getColumnSlice();
			
			Comentario comment = new Comentario();
			
			String identificador = row.getKey();
                        i = Integer.parseInt(identificador);
                        if (i>codigoINT){
                            codigoINT = i;
                        }
                            
                        //codigo = identificador;
                                                                      
			
//			String dateStr = columnSlice.getColumnByName(CN_CREATED_AT).getValue();
//			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			try {
//				Date date = formatter.parse(dateStr);
//				user.setCreated_at(date);
//			} catch (ParseException e) {
//				logger.debug(e.getMessage());
//			}			
                        
		}
                codigo = String.valueOf(codigoINT);
                return codigo;
            
            
        }
        
        @Override
	public int countComentario() {
		CountQuery<String, String> countQuery = HFactory.createCountQuery(keyspace, StringSerializer.get(), StringSerializer.get());
		countQuery.setColumnFamily(CF_NAME);
		QueryResult<Integer> result = countQuery.execute();
		return result.get();
	}
        
        @Override
        public void insertarComentarioSuperColumn(Comentario comment){
            
         serializer = StringSerializer.get();
  
           try {       
               
                   Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);  
                   mutator.insert(comment.getnickName(), "Usuario_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("contenido", comment.getmensaje())),serializer, serializer, serializer));                 
                   mutator.insert(comment.getnickName(), "Usuario_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("fecha_creacion", comment.getFecha_creacion())),serializer, serializer, serializer));                 
                   mutator.insert(comment.getnickName(), "Usuario_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("adjunto", comment.getAdjunto())),serializer, serializer, serializer));                 
                   
                   SuperColumnQuery<String, String, String, String> superColumnQuery = HFactory.createSuperColumnQuery(keyspace,serializer, serializer,serializer, serializer);            
                   superColumnQuery.setColumnFamily("Usuario_Comentario").setKey(comment.getnickName()).setSuperName(comment.getId());            
                   QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();            
//                   System.out.println("Read HSuperColumn from cassandra: " + result.get());                       
//                   System.out.println("Verify on CLI with:  get Proyecto.Usuario_Comentario['sd']['1'] ");               
      
           } catch (HectorException e) {           
               e.printStackTrace();      
           }         
//           cluster.getConnectionManager().shutdown();     
       
            
        }
        
        @Override
        public void borrarComentarioSuperColumn(Comentario comment){
            
           serializer = StringSerializer.get(); 
           Mutator<String> m = createMutator(keyspace, serializer);
           //Mutator m = HFactory.createMutator("Proyecto", serializer);
            System.out.println(comment.getnickName());
            System.out.println(comment.getId());
	   MutationResult mr2 = m.delete(comment.getnickName(), "Usuario_Comentario", comment.getId(), serializer);
            
        }
        
        @Override
        public Collection<Comentario> comentariosReply(String ruta){
       
            serializer = StringSerializer.get();

            List<Comentario> commentList = new ArrayList<Comentario>();
            
            System.out.println("nombre de la variable "+ruta);
            RangeSuperSlicesQuery<String, String, String,String> rangeSuperSlicesQuery =
            HFactory.createRangeSuperSlicesQuery(keyspace, serializer, serializer,serializer,serializer);
            rangeSuperSlicesQuery.setColumnFamily("Reply_Comentario");
            //rangeSuperSlicesQuery.setKeys("", "");
            rangeSuperSlicesQuery.setKeys(ruta, ruta);
            rangeSuperSlicesQuery.setRange("", "", false, 100);
            rangeSuperSlicesQuery.setRowCount(11);
            //rangeSuperSlicesQuery.setColumnNames("");
            QueryResult<OrderedSuperRows<String,String,String,String>> queryResult = rangeSuperSlicesQuery.execute();
            OrderedSuperRows<String,String,String,String> orderedRows = queryResult.get();
            // System.out.print(orderedRows.getList());
            linea = orderedRows.getList().toString();
              // comandos = linea.split(",");
               comandos = linea.split("HColumn\\(");

                cuenta = comandos.length;
                Comentario com = new Comentario();
                for (int i=0; i<cuenta;++i){
                    if (comandos[i].startsWith("adjunto=")) //|| (comandos[i].startsWith("contenido=")) || (comandos[i].startsWith("fecha_creacion=")))
                        {

                            res = comandos[i];
                            // System.out.println("res "+res); 
                            comandos2 = res.split("adjunto=");
                            res2 = comandos2[1];
                            definitivo = res2.split("\\),");
                            //System.out.println("adjunto "+res2);

                //          System.out.println("adjunto final: "+definitivo[e]);
                            ultimoSplit = definitivo[0];
                            arregloFinal[e] = ultimoSplit;
                            com.setAdjunto(ultimoSplit);
                            com.setId("1");
                            System.out.println("adjunto final en pos "+e+": "+ arregloFinal[e]);
                            e++;
           
                        }
               
              if (comandos[i].startsWith("contenido=")) 
              {
                
                 res = comandos[i];
                 // System.out.println("res "+res); 
                 comandos2 = res.split("contenido=");
                 res2 = comandos2[1];
                 definitivo = res2.split("\\),");
                //System.out.println("adjunto "+res2);
                
      //          System.out.println("adjunto final: "+definitivo[e]);
                ultimoSplit = definitivo[0];
                arregloFinal[e] = ultimoSplit;
                com.setmensaje(ultimoSplit);
                System.out.println("contenedor final en pos "+e+": "+ arregloFinal[e]);
                e++;
           
              }  
              
             if (comandos[i].startsWith("fecha_creacion="))
              {
                
                 res = comandos[i];
                 // System.out.println("res "+res); 
                 comandos2 = res.split("fecha_creacion=");
                 res2 = comandos2[1];
                 definitivo = res2.split("\\)]");
                 //System.out.println("adjunto "+res2);
                
      //          System.out.println("adjunto final: "+definitivo[e]);
                ultimoSplit = definitivo[0];
                arregloFinal[e] = ultimoSplit; 
                System.out.println("fecha_creacio final en pos "+e+": "+ arregloFinal[e]);
                e++;
                
                com.setFecha_creacion(ultimoSplit);
                  System.out.println(com.getmensaje());
                commentList.add(com);
                com = instancia();
              }  
             
             }
        //   System.out.println("prueba "+linea.replaceAll(" HColumn","<%"));  
            //  comandos[1].                 
       //  System.out.println("la cuenta de , es:"+cuenta);
//        System.out.print(linea);

        return commentList; 
     
        }
        
        @Override
        public void insertarReplySuperColumn(Comentario comment){
            
         serializer = StringSerializer.get();
  
           try {       
               
                   Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);  
                   mutator.insert(comment.getReply(), "Reply_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("contenido", comment.getmensaje())),serializer, serializer, serializer));                 
                   mutator.insert(comment.getReply(), "Reply_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("fecha_creacion", comment.getFecha_creacion())),serializer, serializer, serializer));                 
                   mutator.insert(comment.getReply(), "Reply_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("adjunto", comment.getAdjunto())),serializer, serializer, serializer));                 
                   
                   SuperColumnQuery<String, String, String, String> superColumnQuery = HFactory.createSuperColumnQuery(keyspace,serializer, serializer,serializer, serializer);            
                   superColumnQuery.setColumnFamily("Reply_Comentario").setKey(comment.getReply()).setSuperName(comment.getId());            
                   QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();            
//                   System.out.println("Read HSuperColumn from cassandra: " + result.get());                       
//                   System.out.println("Verify on CLI with:  get Proyecto.Usuario_Comentario['sd']['1'] ");               
      
           } catch (HectorException e) {           
               e.printStackTrace();      
           }         
//           cluster.getConnectionManager().shutdown();     
       
            
        }
        
        @Override
        public void borrarReplyRaizSuperColumn(String name){
            
           serializer = StringSerializer.get(); 
           Mutator<String> m = createMutator(keyspace, serializer);
           //Mutator m = HFactory.createMutator("Proyecto", serializer);
//            System.out.println(comment.getReply());
//            System.out.println(comment.getId());
	   //MutationResult mr2 = m.delete(comment.getReply(), "Reply_Comentario", comment.getId(), serializer);
            MutationResult mr2 = m.superDelete(name, "Reply_Comentario", null, serializer);
        }
        
        @Override
        public void borrarReplySegundoSuperColumn(Comentario comment){
            
           serializer = StringSerializer.get(); 
           Mutator<String> m = createMutator(keyspace, serializer);
           //Mutator m = HFactory.createMutator("Proyecto", serializer);
            System.out.println("Borrar reply segundo: "+comment.getReply());
            System.out.println("Borrar reply segundo: "+comment.getId());
	   MutationResult mr2 = m.delete(comment.getReply(), "Reply_Comentario", comment.getId(), serializer);
            //MutationResult mr2 = m.superDelete(name, "Reply_Comentario", null, serializer);
        }
        
        @Override
        public void deleteComentarioEtiqueta(String ruta) {

                       serializer = StringSerializer.get(); 
           Mutator<String> m = createMutator(keyspace, serializer);
           //Mutator m = HFactory.createMutator("Proyecto", serializer);
            System.out.println("Borrar de comentario etiqueta: "+ruta);
//            System.out.println("Borrar reply segundo: "+comment.getId());
//	   MutationResult mr2 = m.delete(comment.getReply(), "Reply_Comentario", comment.getId(), serializer);
            MutationResult mr2 = m.superDelete(ruta, "Comentario_Etiqueta", null, serializer);

	}
        
        @Override
        public void insertarComentarioEtiquetaSuperColumn(Etiqueta tag){
            
         serializer = StringSerializer.get();
  
           try {       
               
                   Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);  
                   mutator.insert(tag.getCommentario(), "Comentario_Etiqueta", HFactory.createSuperColumn(tag.getNombre(),Arrays.asList(HFactory.createStringColumn("nombre", tag.getNombre())),serializer, serializer, serializer));                 
//                   mutator.insert(comment.getnickName(), "Usuario_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("fecha_creacion", comment.getFecha_creacion())),serializer, serializer, serializer));                 
//                   mutator.insert(comment.getnickName(), "Usuario_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("adjunto", comment.getAdjunto())),serializer, serializer, serializer));                 
                   
                   SuperColumnQuery<String, String, String, String> superColumnQuery = HFactory.createSuperColumnQuery(keyspace,serializer, serializer,serializer, serializer);            
                   superColumnQuery.setColumnFamily("Comentario_Etiqueta").setKey(tag.getCommentario()).setSuperName(tag.getNombre());            
                   QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();            
//                   System.out.println("Read HSuperColumn from cassandra: " + result.get());                       
//                   System.out.println("Verify on CLI with:  get Proyecto.Usuario_Comentario['sd']['1'] ");               
      
           } catch (HectorException e) {           
               e.printStackTrace();      
           }         
//           cluster.getConnectionManager().shutdown();     
       
            
        }
        
        @Override
        public Collection<Etiqueta> comentarioEtiquetas(String ruta){
       
            serializer = StringSerializer.get();

            List<Etiqueta> tagList = new ArrayList<Etiqueta>();
            
            System.out.println("nombre de la variable "+ruta);
            RangeSuperSlicesQuery<String, String, String,String> rangeSuperSlicesQuery =
            HFactory.createRangeSuperSlicesQuery(keyspace, serializer, serializer,serializer,serializer);
            rangeSuperSlicesQuery.setColumnFamily("Comentario_Etiqueta");
            //rangeSuperSlicesQuery.setKeys("", "");
            rangeSuperSlicesQuery.setKeys(ruta, ruta);
            rangeSuperSlicesQuery.setRange("", "", false, 100);
            rangeSuperSlicesQuery.setRowCount(11);
            //rangeSuperSlicesQuery.setColumnNames("");
            QueryResult<OrderedSuperRows<String,String,String,String>> queryResult = rangeSuperSlicesQuery.execute();
            OrderedSuperRows<String,String,String,String> orderedRows = queryResult.get();
            // System.out.print(orderedRows.getList());
            linea = orderedRows.getList().toString();
              // comandos = linea.split(",");
                System.out.println(linea);
               comandos = linea.split("HColumn\\(");
               
                cuenta = comandos.length;
                Etiqueta com = new Etiqueta();
                for (int i=0; i<cuenta;++i){
                    if (comandos[i].startsWith("nombre=")) //|| (comandos[i].startsWith("contenido=")) || (comandos[i].startsWith("fecha_creacion=")))
                        {
//
                            res = comandos[i];
                             System.out.println("res "+res); 
                            comandos2 = res.split("nombre=");
                            res2 = comandos2[1];
                            definitivo = res2.split("\\)]");
                            System.out.println("adjunto "+res2);
//
                          System.out.println("adjunto final: "+definitivo[e]);
                            ultimoSplit = definitivo[0];
                            arregloFinal[e] = ultimoSplit;
                            com.setNombre(ultimoSplit);
//                            com.setId("1");
                            System.out.println("adjunto final en pos "+e+": "+ arregloFinal[e]);
                            e++;
                            tagList.add(com);
                            com = instanciaEtiqueta();
                            
//           
                        }
//               
//              if (comandos[i].startsWith("contenido=")) 
//              {
//                
//                 res = comandos[i];
//                 // System.out.println("res "+res); 
//                 comandos2 = res.split("contenido=");
//                 res2 = comandos2[1];
//                 definitivo = res2.split("\\),");
//                //System.out.println("adjunto "+res2);
//                
//      //          System.out.println("adjunto final: "+definitivo[e]);
//                ultimoSplit = definitivo[0];
//                arregloFinal[e] = ultimoSplit;
//                com.setmensaje(ultimoSplit);
//                System.out.println("contenedor final en pos "+e+": "+ arregloFinal[e]);
//                e++;
//           
//              }  
//              
//             if (comandos[i].startsWith("fecha_creacion="))
//              {
//                
//                 res = comandos[i];
//                 // System.out.println("res "+res); 
//                 comandos2 = res.split("fecha_creacion=");
//                 res2 = comandos2[1];
//                 definitivo = res2.split("\\)]");
//                 //System.out.println("adjunto "+res2);
//                
//      //          System.out.println("adjunto final: "+definitivo[e]);
//                ultimoSplit = definitivo[0];
//                arregloFinal[e] = ultimoSplit; 
//                System.out.println("fecha_creacio final en pos "+e+": "+ arregloFinal[e]);
//                e++;
//                
//                com.setFecha_creacion(ultimoSplit);
//                  System.out.println(com.getmensaje());
////                commentList.add(com);
//                com = instancia();
//              }  
//             
             }
        //   System.out.println("prueba "+linea.replaceAll(" HColumn","<%"));  
            //  comandos[1].                 
       //  System.out.println("la cuenta de , es:"+cuenta);
//        System.out.print(linea);

        return tagList; 
     
        }
        
        @Override
        public void insertarComentarioPuntuacionSuperColumn(Comentario comment){
            
            serializer = StringSerializer.get();
  
           try {       
               
                   Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);  
                   mutator.insert(comment.getId(), "Puntuacion_Comentario", HFactory.createSuperColumn(comment.getnickName(),Arrays.asList(HFactory.createStringColumn("punto", comment.getAdjunto())),serializer, serializer, serializer));                 
//                   mutator.insert(comment.getnickName(), "Usuario_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("fecha_creacion", comment.getFecha_creacion())),serializer, serializer, serializer));                 
//                   mutator.insert(comment.getnickName(), "Usuario_Comentario", HFactory.createSuperColumn(comment.getId(),Arrays.asList(HFactory.createStringColumn("adjunto", comment.getAdjunto())),serializer, serializer, serializer));                 
                   
                   SuperColumnQuery<String, String, String, String> superColumnQuery = HFactory.createSuperColumnQuery(keyspace,serializer, serializer,serializer, serializer);            
                   superColumnQuery.setColumnFamily("Puntuacion_Comentario").setKey(comment.getId()).setSuperName(comment.getnickName());            
                   QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();            
//                   System.out.println("Read HSuperColumn from cassandra: " + result.get());                       
//                   System.out.println("Verify on CLI with:  get Proyecto.Usuario_Comentario['sd']['1'] ");               
      
           } catch (HectorException e) {           
               e.printStackTrace();      
           } 
            
        }
        
        @Override
        public void deleteComentarioPuntuacion(String ruta) {

           serializer = StringSerializer.get(); 
           Mutator<String> m = createMutator(keyspace, serializer);
           //Mutator m = HFactory.createMutator("Proyecto", serializer);
            System.out.println("Borrar de comentario puntuacion: "+ruta);
//            System.out.println("Borrar reply segundo: "+comment.getId());
//	   MutationResult mr2 = m.delete(comment.getReply(), "Reply_Comentario", comment.getId(), serializer);
            MutationResult mr2 = m.superDelete(ruta, "Puntuacion_Comentario", null, serializer);

	}
        
        @Override
        public Collection<Etiqueta> comentarioPuntuacionMeGusta(String ruta){
       
            serializer = StringSerializer.get();

            List<Etiqueta> tagList = new ArrayList<Etiqueta>();
            
            System.out.println("nombre de la variable "+ruta);
            RangeSuperSlicesQuery<String, String, String,String> rangeSuperSlicesQuery =
            HFactory.createRangeSuperSlicesQuery(keyspace, serializer, serializer,serializer,serializer);
            rangeSuperSlicesQuery.setColumnFamily("Puntuacion_Comentario");
            //rangeSuperSlicesQuery.setKeys("", "");
            rangeSuperSlicesQuery.setKeys(ruta, ruta);
            rangeSuperSlicesQuery.setRange("", "", false, 100);
            rangeSuperSlicesQuery.setRowCount(11);
            //rangeSuperSlicesQuery.setColumnNames("");
            QueryResult<OrderedSuperRows<String,String,String,String>> queryResult = rangeSuperSlicesQuery.execute();
            OrderedSuperRows<String,String,String,String> orderedRows = queryResult.get();
            // System.out.print(orderedRows.getList());
            linea = orderedRows.getList().toString();
              // comandos = linea.split(",");
                System.out.println(linea);
               comandos = linea.split("HColumn\\(");
               
                cuenta = comandos.length;
                Etiqueta com = new Etiqueta();
                for (int i=0; i<cuenta;++i){
                    if (comandos[i].startsWith("punto=")) //|| (comandos[i].startsWith("contenido=")) || (comandos[i].startsWith("fecha_creacion=")))
                        {
//
                            res = comandos[i];
                             System.out.println("res "+res); 
                            comandos2 = res.split("punto=");
                            res2 = comandos2[1];
                            definitivo = res2.split("\\)]");
                            System.out.println("punto: "+res2);
//
                          System.out.println("punto final: "+definitivo[e]);
                            ultimoSplit = definitivo[0];
                            arregloFinal[e] = ultimoSplit;
                            com.setNombre(ultimoSplit);
//                            com.setId("1");
                            System.out.println("adjunto final en pos "+e+": "+ arregloFinal[e]);
                            e++;
                            if (ultimoSplit.equals("1")){
                                tagList.add(com);
                                com = instanciaEtiqueta();
                            }
//           
                        }
//               
//              if (comandos[i].startsWith("contenido=")) 
//              {
//                
//                 res = comandos[i];
//                 // System.out.println("res "+res); 
//                 comandos2 = res.split("contenido=");
//                 res2 = comandos2[1];
//                 definitivo = res2.split("\\),");
//                //System.out.println("adjunto "+res2);
//                
//      //          System.out.println("adjunto final: "+definitivo[e]);
//                ultimoSplit = definitivo[0];
//                arregloFinal[e] = ultimoSplit;
//                com.setmensaje(ultimoSplit);
//                System.out.println("contenedor final en pos "+e+": "+ arregloFinal[e]);
//                e++;
//           
//              }  
//              
//             if (comandos[i].startsWith("fecha_creacion="))
//              {
//                
//                 res = comandos[i];
//                 // System.out.println("res "+res); 
//                 comandos2 = res.split("fecha_creacion=");
//                 res2 = comandos2[1];
//                 definitivo = res2.split("\\)]");
//                 //System.out.println("adjunto "+res2);
//                
//      //          System.out.println("adjunto final: "+definitivo[e]);
//                ultimoSplit = definitivo[0];
//                arregloFinal[e] = ultimoSplit; 
//                System.out.println("fecha_creacio final en pos "+e+": "+ arregloFinal[e]);
//                e++;
//                
//                com.setFecha_creacion(ultimoSplit);
//                  System.out.println(com.getmensaje());
////                commentList.add(com);
//                com = instancia();
//              }  
//             
             }
        //   System.out.println("prueba "+linea.replaceAll(" HColumn","<%"));  
            //  comandos[1].                 
       //  System.out.println("la cuenta de , es:"+cuenta);
//        System.out.print(linea);

        return tagList; 
     
        }
        
        @Override
        public Collection<Etiqueta> comentarioPuntuacionNoMeGusta(String ruta){
       
            serializer = StringSerializer.get();

            List<Etiqueta> tagList = new ArrayList<Etiqueta>();
            
            System.out.println("nombre de la variable "+ruta);
            RangeSuperSlicesQuery<String, String, String,String> rangeSuperSlicesQuery =
            HFactory.createRangeSuperSlicesQuery(keyspace, serializer, serializer,serializer,serializer);
            rangeSuperSlicesQuery.setColumnFamily("Puntuacion_Comentario");
            //rangeSuperSlicesQuery.setKeys("", "");
            rangeSuperSlicesQuery.setKeys(ruta, ruta);
            rangeSuperSlicesQuery.setRange("", "", false, 100);
            rangeSuperSlicesQuery.setRowCount(11);
            //rangeSuperSlicesQuery.setColumnNames("");
            QueryResult<OrderedSuperRows<String,String,String,String>> queryResult = rangeSuperSlicesQuery.execute();
            OrderedSuperRows<String,String,String,String> orderedRows = queryResult.get();
            // System.out.print(orderedRows.getList());
            linea = orderedRows.getList().toString();
              // comandos = linea.split(",");
                System.out.println(linea);
               comandos = linea.split("HColumn\\(");
               
                cuenta = comandos.length;
                Etiqueta com = new Etiqueta();
                for (int i=0; i<cuenta;++i){
                    if (comandos[i].startsWith("punto=")) //|| (comandos[i].startsWith("contenido=")) || (comandos[i].startsWith("fecha_creacion=")))
                        {
//
                            res = comandos[i];
                             System.out.println("res "+res); 
                            comandos2 = res.split("punto=");
                            res2 = comandos2[1];
                            definitivo = res2.split("\\)]");
                            System.out.println("punto: "+res2);
//
                          System.out.println("punto final: "+definitivo[e]);
                            ultimoSplit = definitivo[0];
                            arregloFinal[e] = ultimoSplit;
                            com.setNombre(ultimoSplit);
//                            com.setId("1");
                            System.out.println("adjunto final en pos "+e+": "+ arregloFinal[e]);
                            e++;
                            if (ultimoSplit.equals("0")){
                                tagList.add(com);
                                com = instanciaEtiqueta();
                            }
//           
                        }
//               
//              if (comandos[i].startsWith("contenido=")) 
//              {
//                
//                 res = comandos[i];
//                 // System.out.println("res "+res); 
//                 comandos2 = res.split("contenido=");
//                 res2 = comandos2[1];
//                 definitivo = res2.split("\\),");
//                //System.out.println("adjunto "+res2);
//                
//      //          System.out.println("adjunto final: "+definitivo[e]);
//                ultimoSplit = definitivo[0];
//                arregloFinal[e] = ultimoSplit;
//                com.setmensaje(ultimoSplit);
//                System.out.println("contenedor final en pos "+e+": "+ arregloFinal[e]);
//                e++;
//           
//              }  
//              
//             if (comandos[i].startsWith("fecha_creacion="))
//              {
//                
//                 res = comandos[i];
//                 // System.out.println("res "+res); 
//                 comandos2 = res.split("fecha_creacion=");
//                 res2 = comandos2[1];
//                 definitivo = res2.split("\\)]");
//                 //System.out.println("adjunto "+res2);
//                
//      //          System.out.println("adjunto final: "+definitivo[e]);
//                ultimoSplit = definitivo[0];
//                arregloFinal[e] = ultimoSplit; 
//                System.out.println("fecha_creacio final en pos "+e+": "+ arregloFinal[e]);
//                e++;
//                
//                com.setFecha_creacion(ultimoSplit);
//                  System.out.println(com.getmensaje());
////                commentList.add(com);
//                com = instancia();
//              }  
//             
             }
        //   System.out.println("prueba "+linea.replaceAll(" HColumn","<%"));  
            //  comandos[1].                 
       //  System.out.println("la cuenta de , es:"+cuenta);
//        System.out.print(linea);

        return tagList; 
     
        }
    
}
