/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

/**
 *
 * @author Antonio
 */

import java.util.Calendar;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import prueba.DAO.*;
import prueba.modelo.Comentario;
import prueba.modelo.Etiqueta;
import prueba.modelo.Tokken;
import prueba.modelo.Usuario;


@Controller
@RequestMapping("/borrar/usuario")
public class borrarUsuario {
    
    private usuarioDAO dao;
    
    private tokkenDAO daoTok;
    private etiquetaDAO daoTag;
    private String arreglo[];
    private String diasss[];
    private String horasss[];
    private Calendar calendario;
    String horas, minutos;
    int month;
    String dia, mes, anno, fechaCon;
    
    	@RequestMapping(value="{name}", method = RequestMethod.DELETE)
	public @ResponseBody Usuario borrarUsuario(@PathVariable String name) {
            
            
            dao = new usuarioDAOImpl();
            
            Usuario user = new Usuario();
                   
            user = dao.findUsuario(name);
            
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
                            
                               dao.deleteUsuario(name);
                               dao.borrarUsuarioSuperColumn(name);
                
                               System.out.println("hola3");
		
                               return user;
                            
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
            
//////////////////                System.out.println("holaaaa");
//////////////////            
//////////////////		Usuario user = new Usuario();
//////////////////                
//////////////////                dao = new usuarioDAOImpl();
//////////////////                
//////////////////                System.out.println("holaaaa2");
//////////////////                System.out.println(name);
//////////////////                
//////////////////                user = dao.findUsuario(name);
////////////////////                if (user.getNickname() != null)
//////////////////                dao.deleteUsuario(name);
//////////////////                dao.borrarUsuarioSuperColumn(name);
//////////////////                
//////////////////                System.out.println("hola3");
//////////////////		
//////////////////		return user;

	}
    
}
