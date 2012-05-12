/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

import java.io.StringReader;
import java.util.Calendar;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.Marshaller;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import prueba.DAO.*;
import prueba.modelo.Tokken;
import prueba.modelo.Usuario;

/**
 *
 * @author Antonio
 */

@Controller
public class updateUsuario {
    
    private usuarioDAO dao;
    private tokkenDAO daoTok;
    private String arreglo[];
    private String diasss[];
    private String horasss[];
    private Calendar calendario;
    String horas, minutos;
    int month;
    String dia, mes, anno, fechaCon;
    
    @RequestMapping(value = "/updateUsuario", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody Usuario updateUsuarioInXML(@RequestBody Usuario entrada) {
            
                if (entrada.getNickname().isEmpty()){
                   entrada.setNickname("ERROR 001: Nickname nulo");
                   entrada.setApellido("ERROR 001");
                   entrada.setBiografia("ERROR 001");
                   entrada.setClave("ERROR 001");
                   entrada.setCorreo("ERROR 001");
                   entrada.setFecha_nac("ERROR 001");
                   entrada.setFoto("ERROR 001");
                   entrada.setNombre("ERROR 001");
                   entrada.setPais("ERROR 001");
                  
                   return entrada;
               } 
               else
                    if (entrada.getApellido().isEmpty()){
                   entrada.setNickname("ERROR 002");
                   entrada.setApellido("ERROR 002: Apellido nulo");
                   entrada.setBiografia("ERROR 002");
                   entrada.setClave("ERROR 002");
                   entrada.setCorreo("ERROR 002");
                   entrada.setFecha_nac("ERROR 002");
                   entrada.setFoto("ERROR 002");
                   entrada.setNombre("ERROR 002");
                   entrada.setPais("ERROR 002");
                  
                   return entrada;
               } 
               else
                    if (entrada.getBiografia().isEmpty()){
                   entrada.setNickname("ERROR 003");
                   entrada.setApellido("ERROR 003");
                   entrada.setBiografia("ERROR 003: Biografia nulo");
                   entrada.setClave("ERROR 003");
                   entrada.setCorreo("ERROR 003");
                   entrada.setFecha_nac("ERROR 003");
                   entrada.setFoto("ERROR 003");
                   entrada.setNombre("ERROR 003");
                   entrada.setPais("ERROR 003");
                  
                   return entrada;
               } 
                 else
                    if (entrada.getClave().isEmpty()){
                   entrada.setNickname("ERROR 004");
                   entrada.setApellido("ERROR 004");
                   entrada.setBiografia("ERROR 004");
                   entrada.setClave("ERROR 004: Clave nulo");
                   entrada.setCorreo("ERROR 004");
                   entrada.setFecha_nac("ERROR 004");
                   entrada.setFoto("ERROR 004");
                   entrada.setNombre("ERROR 004");
                   entrada.setPais("ERROR 004");
                  
                   return entrada;
               }    
                else
                    if (entrada.getCorreo().isEmpty()){
                   entrada.setNickname("ERROR 005");
                   entrada.setApellido("ERROR 005");
                   entrada.setBiografia("ERROR 005");
                   entrada.setClave("ERROR 005");
                   entrada.setCorreo("ERROR 005: Correo nulo");
                   entrada.setFecha_nac("ERROR 005");
                   entrada.setFoto("ERROR 005");
                   entrada.setNombre("ERROR 005");
                   entrada.setPais("ERROR 005");
                  
                   return entrada;
               }         
                   else
                    if (entrada.getFecha_nac().isEmpty()){
                   entrada.setNickname("ERROR 006");
                   entrada.setApellido("ERROR 006");
                   entrada.setBiografia("ERROR 006");
                   entrada.setClave("ERROR 006");
                   entrada.setCorreo("ERROR 006");
                   entrada.setFecha_nac("ERROR 006: Fecha nulo");
                   entrada.setFoto("ERROR 006");
                   entrada.setNombre("ERROR 006");
                   entrada.setPais("ERROR 006");
                  
                   return entrada;
               }      
                  else
                    if (entrada.getNombre().isEmpty()){
                   entrada.setNickname("ERROR 007");
                   entrada.setApellido("ERROR 007");
                   entrada.setBiografia("ERROR 007");
                   entrada.setClave("ERROR 007");
                   entrada.setCorreo("ERROR 007");
                   entrada.setFecha_nac("ERROR 007");
                   entrada.setFoto("ERROR 007");
                   entrada.setNombre("ERROR 007: Nombre nulo");
                   entrada.setPais("ERROR 007");
                  
                   return entrada;
               }       
                   
                 else
                    if (entrada.getFoto().isEmpty()){
                 
                   entrada.setFoto("no");
                   //dao.insertUsuario(entrada);

                  
                   return entrada;
               }
                  else
                    if (entrada.getPais().isEmpty()){
                   entrada.setNickname("ERROR 008");
                   entrada.setApellido("ERROR 008");
                   entrada.setBiografia("ERROR 008");
                   entrada.setClave("ERROR 008");
                   entrada.setCorreo("ERROR 008");
                   entrada.setFecha_nac("ERROR 008");
                   entrada.setFoto("ERROR 008");
                   entrada.setNombre("ERROR 008");
                   entrada.setPais("ERROR 008: Pais nulo");
                  
                   return entrada;
               }
                
        
              dao = new usuarioDAOImpl();
            
            Usuario user = new Usuario();
                   
            user = dao.findUsuario(entrada.getNickname());
            System.out.println("Nickname: "+user.getNickname().toString());
            if (!user.getNickname().isEmpty()){
                       
                       Tokken findTokenViejo = new Tokken();
               
                       daoTok = new tokkenDAOImpl();
               
                       findTokenViejo = daoTok.findTokken(user.getNickname());
                   
               
               if ((!findTokenViejo.getTokken().isEmpty())) {
                    
                   if(findTokenViejo.getUsuario().equals(user.getNickname())) {
                   
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
                            
                             dao.insertUsuario(entrada);                
		
                             return entrada;
                            
                        }
                        else {
                            
                            System.out.println("Token vencido");
                            user.setApellido("ERROR 400");
                            user.setBiografia("ERROR 400");
                            user.setClave("ERROR 400");
                            user.setCorreo("ERROR 400");
                            user.setFecha_nac("ERROR 400");
                            user.setFoto("ERROR 400");
                            user.setNickname("ERROR 400: Token vencido");
                            user.setNombre("ERROR 400");
                            user.setPais("ERROR 400");
                            
                            return user;
                            
                        }
                        
                    }
                    else{
                            System.out.println("Token vencido");
                            user.setApellido("ERROR 400");
                            user.setBiografia("ERROR 400");
                            user.setClave("ERROR 400");
                            user.setCorreo("ERROR 400");
                            user.setFecha_nac("ERROR 400");
                            user.setFoto("ERROR 400");
                            user.setNickname("ERROR 400: Token vencido");
                            user.setNombre("ERROR 400");
                            user.setPais("ERROR 400");
                            
                            return user;
                    }
                   } else {
                            System.out.println("El token no corresponde con el usuario");
                            user.setApellido("ERROR 150");
                            user.setBiografia("ERROR 150");
                            user.setClave("ERROR 150");
                            user.setCorreo("ERROR 150");
                            user.setFecha_nac("ERROR 150");
                            user.setFoto("ERROR 150");
                            user.setNickname("ERROR 150: El token no corresponde con el usuario");
                            user.setNombre("ERROR 150");
                            user.setPais("ERROR 150");
                            
                            return user;
                       
                   }
                   
               }
               
                            System.out.println("Token incorrecto");
                            user.setApellido("ERROR 111");
                            user.setBiografia("ERROR 111");
                            user.setClave("ERROR 111");
                            user.setCorreo("ERROR 111");
                            user.setFecha_nac("ERROR 111");
                            user.setFoto("ERROR 111");
                            user.setNickname("ERROR 111: Token incorrecto");
                            user.setNombre("ERROR 111");
                            user.setPais("ERROR 111");
                       
                            return user;
                   }
                   else{
                       
                        user.setApellido("ERROR 222");
                            user.setBiografia("ERROR 222");
                            user.setClave("ERROR 222");
                            user.setCorreo("ERROR 222");
                            user.setFecha_nac("ERROR 222");
                            user.setFoto("ERROR 222");
                            user.setNickname("ERROR 222: El usuario no existe en el sistema");
                            user.setNombre("ERROR 222");
                            user.setPais("ERROR 222");
                        
                        return user;
                       
                   }
        
                
////////////////                dao.insertUsuario(entrada);
////////////////
////////////////                //user = dao.findUsuario(e.getNickname());                
////////////////		
////////////////		return entrada;

	}
    
}
