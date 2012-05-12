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
@RequestMapping("/borrar/comentario")
public class borrarComentario {
    
    private comentarioDAO dao;
    
    private usuarioDAO daoUser;
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
	public @ResponseBody Comentario borrarComentario(@PathVariable String name) {
            
                dao = new comentarioDAOImpl();
                
                daoUser = new usuarioDAOImpl();
                
               Comentario existe = new Comentario();
               
               existe = dao.findComentario(name);
                
               
               if(!existe.getId().isEmpty()){
                   
                   Usuario user = new Usuario();
                   
                   user = daoUser.findUsuario(existe.getnickName());
                   
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
                            
                            dao.deleteComentario(name);
                            dao.borrarComentarioSuperColumn(existe);
                            dao.borrarReplyRaizSuperColumn(name);
                            dao.deleteComentarioPuntuacion(name);

                            if (!existe.getReply().equals("0")){
                                dao.borrarReplySegundoSuperColumn(existe);
                            }

                            dao.deleteComentarioEtiqueta(name);

                            System.out.println("hola3");

                            return existe;
                            
                        }
                        else {
                            
                            System.out.println("Token vencido");
                            existe.setId("ERROR 100: Token vencido");
                            existe.setAdjunto("ERROR 100");
                            existe.setFecha_creacion("ERROR 100");
                            existe.setReply("ERROR 100");
                            existe.setmensaje("ERROR 100");
                            existe.setnickName("ERROR 100");
                            
                            return existe;
                            
                        }
                        
                    }
                    else{
                            System.out.println("Token vencido");
                            existe.setId("ERROR 100: Token vencido");
                            existe.setAdjunto("ERROR 100");
                            existe.setFecha_creacion("ERROR 100");
                            existe.setReply("ERROR 100");
                            existe.setmensaje("ERROR 100");
                            existe.setnickName("ERROR 100");
                            
                            return existe;
                    }
                   } else {
                            System.out.println("El token no corresponde con el usuario");
                            existe.setId("ERROR 150: El token no corresponde con el usuario");
                            existe.setAdjunto("ERROR 150");
                            existe.setFecha_creacion("ERROR 150");
                            existe.setReply("ERROR 150");
                            existe.setmensaje("ERROR 150");
                            existe.setnickName("ERROR 150");
                            
                            return existe;
                       
                   }
                   
               }
               
                            System.out.println("Token incorrecto");
                            existe.setId("ERROR 111: Token incorrecto");
                            existe.setAdjunto("ERROR 111");
                            existe.setFecha_creacion("ERROR 111");
                            existe.setReply("ERROR 111");
                            existe.setmensaje("ERROR 111");
                            existe.setnickName("ERROR 111");
                       
                            return existe;
                   }
                   else{
                       
                            existe.setId("ERROR 019: Usuario no existe en el sistema");
                            existe.setAdjunto("ERROR 019");
                            existe.setFecha_creacion("ERROR 019");
                            existe.setReply("ERROR 019");
                            existe.setmensaje("ERROR 019");
                            existe.setnickName("ERROR 019");
                        
                        return existe;
                       
                   }
               }
               else{
                   
                            System.out.println("El comentario no existe");
                            existe.setId("ERROR 999: El comentario no existe");
                            existe.setAdjunto("ERROR 999");
                            existe.setFecha_creacion("ERROR 999");
                            existe.setReply("ERROR 999");
                            existe.setmensaje("ERROR 999");
                            existe.setnickName("ERROR 999");
                            
                            return existe;
                   
               }


	}
    
}
