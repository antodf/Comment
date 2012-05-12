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
import prueba.modelo.Comentario;
import prueba.modelo.Etiqueta;
import prueba.modelo.Tokken;
import prueba.modelo.Usuario;

/**
 *
 * @author Antonio
 */

@Controller
public class insertarComentarioEtiqueta {
    
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
    
    @RequestMapping(value = "/comentarioEtiqueta", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody Etiqueta insertarComentarioInXML(@RequestBody Etiqueta entrada) {
        
                 System.out.println("hola comentarioEtiqueta");
            
                if (entrada.getNombre().isEmpty()){
                   entrada.setNombre("ERROR 017: Nombre de etiqueta Nulo");
                   entrada.setCommentario("ERROR 017");
                   entrada.setTokken("ERROR 017");
                 
                  
                   return entrada;
               }   
               else
                 if (entrada.getCommentario().isEmpty()){
                   entrada.setNombre("ERROR 018");
                   entrada.setCommentario("ERROR 018: Comentario Nulo");
                   entrada.setTokken("ERROR 018");
                 
                  
                   return entrada;
               }
                else
                 if (entrada.getTokken().isEmpty()){
                   entrada.setNombre("ERROR 019");
                   entrada.setCommentario("ERROR 019");
                   entrada.setTokken("ERROR 019: Token vacio");
                 
                  
                   return entrada;
               }
                
                dao = new comentarioDAOImpl();
                
                daoUser = new usuarioDAOImpl();
                
               Comentario existe = new Comentario();
               
               existe = dao.findComentario(entrada.getCommentario());
                
               
               if(!existe.getId().isEmpty()){
                   
                   Usuario user = new Usuario();
                   
                   user = daoUser.findUsuario(existe.getnickName());
                   
                   if (!user.getNickname().isEmpty()){
                       
                       Tokken findTokenViejo = new Tokken();
               
                       daoTok = new tokkenDAOImpl();
               
                       findTokenViejo = daoTok.findTokken(user.getNickname());
                   
               
               if ((!findTokenViejo.getTokken().isEmpty())&&(findTokenViejo.getTokken().equals(entrada.getTokken()))) {
                    
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
                            
                            Etiqueta tagg = new Etiqueta();
                            daoTag = new etiquetaDAOImpl();
                            
                            tagg = daoTag.findEtiqueta(entrada.getNombre());
                            
                            if (!tagg.getNombre().isEmpty()){
                            
                                Comentario comment = new Comentario();
                
                                dao = new comentarioDAOImpl();
                
                                dao.insertarComentarioEtiquetaSuperColumn(entrada);
                
                                return entrada;
                            }
                            else {
                                
                                entrada.setNombre("ERROR 119: La etiqueta no existe");
                                entrada.setCommentario("ERROR 119");
                                entrada.setTokken("ERROR 119");
                                
                                return entrada;
                                
                            }
                            
                        }
                        else {
                            
                            System.out.println("Token vencido");
                            entrada.setNombre("ERROR 100");
                            entrada.setCommentario("ERROR 100");
                            entrada.setTokken("ERROR 100: Token vencido");
                            
                            return entrada;
                            
                        }
                        
                    }
                    else{
                            System.out.println("Token vencido");
                            entrada.setNombre("ERROR 100");
                            entrada.setCommentario("ERROR 100");
                            entrada.setTokken("ERROR 100: Token vencido");
                            
                            return entrada;
                    }
                   } else {
                            System.out.println("El token no corresponde con el usuario");
                            entrada.setNombre("ERROR 150");
                            entrada.setCommentario("ERROR 150");
                            entrada.setTokken("ERROR 150: El token no corresponde con el usuario");
                            
                            return entrada;
                       
                   }
                   
               }
               
                            System.out.println("Token incorrecto");
                            entrada.setNombre("ERROR 111");
                            entrada.setCommentario("ERROR 111");
                            entrada.setTokken("ERROR 111: Token incorrecto");
                       
                            return entrada;
                   }
                   else{
                       
                        entrada.setNombre("ERROR 019: Usuario no existe en el sistema");
                        entrada.setCommentario("ERROR 019");
                        entrada.setTokken("ERROR 019");
                        
                        return entrada;
                       
                   }
               }
               else{
                   
                            System.out.println("El comentario no existe");
                            entrada.setNombre("ERROR 999");
                            entrada.setCommentario("ERROR 999: El comentario no existe");
                            entrada.setTokken("ERROR 999");
                            
                            return entrada;
                   
               }

//////////                System.out.println(entrada.toString());
//////////
//////////		Comentario comment = new Comentario();
//////////                
//////////                dao = new comentarioDAOImpl();
//////////                
//////////                dao.insertarComentarioEtiquetaSuperColumn(entrada);
//////////                
//////////            return entrada;
        }
    
}
