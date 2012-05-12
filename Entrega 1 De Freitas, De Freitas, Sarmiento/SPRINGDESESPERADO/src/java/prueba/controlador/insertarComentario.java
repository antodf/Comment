/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

import java.io.StringReader;
import java.util.Calendar;
import java.util.Random;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.Marshaller;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import prueba.DAO.*;
import prueba.modelo.Comentario;
import prueba.modelo.Tokken;
import prueba.modelo.Usuario;

/**
 *
 * @author Antonio
 */
@Controller
public class insertarComentario {
    
    private comentarioDAO dao;
    private tokkenDAO daoTok;
    private String arreglo[];
    private String diasss[];
    private String horasss[];
    private Calendar calendario;
    String horas, minutos;
    int month;
    String dia, mes, anno, fechaCon;
    
    @RequestMapping(value = "/insertarComentario", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody Comentario insertarComentarioInXML(@RequestBody Comentario entrada) {
            
                System.out.println("hola InsertarComentario");

//                StreamSource source = new StreamSource(new StringReader(entrada));
//                Jaxb2Marshaller prueba = new Jaxb2Marshaller();
                System.out.println(entrada.toString());
//                Usuario e = (Usuario) prueba.unmarshal(source);
//                System.out.println(entrada.toString());
                //System.out.println(jaxb2Marshaller.unmarshal(source));
//                Usuario e = (Usuario) jaxb2Marshaller.unmarshal(source);

		Comentario comment = new Comentario();
                
                dao = new comentarioDAOImpl();
                System.out.println("Fecha: "+ entrada.getFecha_creacion());
                System.out.println("Adjunto: "+ entrada.getAdjunto());                
                System.out.println("Contenido: "+ entrada.getmensaje());
                System.out.println("Nickname: "+ entrada.getnickName());
                
                if (entrada.getId().isEmpty()){
                   entrada.setId("ERROR 090: ID token nulo");
                   entrada.setFecha_creacion("ERROR 0090");
                   entrada.setAdjunto("ERROR 090");
                   entrada.setmensaje("ERROR 090");
                   entrada.setnickName("ERROR 090");
                 
                  
                   return entrada;
               } 
               else
                if (entrada.getFecha_creacion().isEmpty()){
                   entrada.setId("ERROR 009");
                   entrada.setFecha_creacion("ERROR 009: Fecha nulo");
                   entrada.setAdjunto("ERROR 009");
                   entrada.setmensaje("ERROR 009");
                   entrada.setnickName("ERROR 009");
                 
                  
                   return entrada;
               } 
               else
                    if (entrada.getAdjunto().isEmpty()){
                   
                   entrada.setAdjunto("NO");
  
               } 
               else
                    if (entrada.getmensaje().isEmpty()){
                        entrada.setId("ERROR 010");
                       entrada.setFecha_creacion("ERROR 010");
                       entrada.setAdjunto("ERROR 010");
                       entrada.setmensaje("ERROR 010: Mensage Nulo");
                       entrada.setnickName("ERROR 010");
                   return entrada;
               } 
                 else
                    if (entrada.getnickName().isEmpty()){
                        entrada.setId("ERROR 011");
                        entrada.setFecha_creacion("ERROR 011");
                        entrada.setAdjunto("ERROR 011");
                        entrada.setmensaje("ERROR 011");
                        entrada.setnickName("ERROR 011: NickName Nulo");
                  
                   return entrada;
               }
                
                //String tok = entrada.getId();
                
                Tokken findTokenViejo = new Tokken();
               
               daoTok = new tokkenDAOImpl();
               
               findTokenViejo = daoTok.findTokken(entrada.getnickName());
               
               if ((!findTokenViejo.getTokken().isEmpty())&&(findTokenViejo.getTokken().equals(entrada.getId()))) {
                    
                   if(findTokenViejo.getUsuario().equals(entrada.getnickName())) {
                   
                    calendario = Calendar.getInstance();
                
                    horas = Integer.toString(calendario.get(Calendar.HOUR_OF_DAY));
                    minutos = Integer.toString(calendario.get(Calendar.MINUTE));
                    dia = Integer.toString(calendario.get(Calendar.DATE));
                    month = calendario.get(Calendar.MONTH);
                    month = month + 1;
                    mes = Integer.toString(month);
                    anno = Integer.toString(calendario.get(Calendar.YEAR));
                
                    fechaCon = dia+"/"+mes+"/"+anno+","+horas+":"+minutos;
                    
                    String fechaVieja = findTokenViejo.getFecha_creacion();
                    
                    arreglo = fechaVieja.split(",");
                    String days = arreglo[0];
                    String hours = arreglo[1];
                    
                    diasss = days.split("/");
                    String diaViejo = diasss[0];
                    String mesViejo = diasss[1];
                    String annoViejo = diasss[2];
                    
                    horasss = hours.split(":");
                    String horaViejo = horasss[0];
                    String minutoViejo = horasss[1];
                    
                    if ((dia.equals(diaViejo)) && (mes.equals(mesViejo)) && (anno.equals(annoViejo))) {
                        
                        int newHora = Integer.parseInt(horas);
                        int oldHora = Integer.parseInt(horaViejo);
                        
                        int oldMinutos = Integer.parseInt(minutoViejo);
                        
                        newHora = newHora*60;
                        oldHora = oldHora*60;
                        
                        newHora = newHora + calendario.get(Calendar.MINUTE);
                        oldHora = oldHora + oldMinutos;
                        
                        int total = newHora - oldHora;
                        
                        if (total <= 3){   //AQUI CAMBIAR PARA LA DURACION DE LOS MINUTOS
                            
                            String idD = dao.getID();
                            System.out.println("ID mas grande de la base de datos: "+idD);

                            int codigoID;
                            codigoID = Integer.parseInt(idD);
                            codigoID = codigoID + 1;

                            idD = String.valueOf(codigoID);
                            System.out.println("ID a enviar al dao: "+idD);
                            entrada.setId(idD);
                            entrada.setReply("0");


                            dao.insertComentario(entrada);
                            dao.insertarComentarioSuperColumn(entrada);

                            //user = dao.findUsuario(e.getNickname());                

                            return entrada;             
                            
                        }
                        else {
                            
                            System.out.println("Token vencido");
                            entrada.setId("ERROR 100: Token vencido");
                            entrada.setFecha_creacion("ERROR 100");
                            entrada.setAdjunto("ERROR 100");
                            entrada.setmensaje("ERROR 100");
                            entrada.setnickName("ERROR 100");
                            
                            return entrada;
                            
                        }
                        
                    }
                    else{
                            System.out.println("Token vencido");
                            entrada.setId("ERROR 100: Token vigente");
                            entrada.setFecha_creacion("ERROR 100");
                            entrada.setAdjunto("ERROR 100");
                            entrada.setmensaje("ERROR 100");
                            entrada.setnickName("ERROR 100");
                            
                            return entrada;
                    }
                   } else {
                            System.out.println("El token no corresponde con el usuario");
                            entrada.setId("ERROR 150: El token no corresponde con el usuario");
                            entrada.setFecha_creacion("ERROR 150");
                            entrada.setAdjunto("ERROR 150");
                            entrada.setmensaje("ERROR 150");
                            entrada.setnickName("ERROR 150");
                            
                            return entrada;
                       
                   }
                   
               }
               
                            System.out.println("Token incorrecto");
                            entrada.setId("ERROR 111: Token incorrecto");
                            entrada.setFecha_creacion("ERROR 111");
                            entrada.setAdjunto("ERROR 111");
                            entrada.setmensaje("ERROR 111");
                            entrada.setnickName("ERROR 111");
                
                
                
          return entrada;    

	}
    
}
