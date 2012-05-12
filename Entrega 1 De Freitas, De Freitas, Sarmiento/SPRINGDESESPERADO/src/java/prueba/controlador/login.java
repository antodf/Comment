/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

import java.io.StringReader;
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
import prueba.modelo.Usuario;
import com.sun.xml.fastinfoset.stax.events.Util;
import java.util.Calendar;
import java.util.Random;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import prueba.modelo.Tokken;
import prueba.modelo.usuarioLogin;

/**
 *
 * @author Antonio
 */
@Controller
public class login {
    
    private usuarioDAO dao;
    private tokkenDAO daoTok;
    private Usuario obj;
    private String arreglo[];
    private String diasss[];
    private String horasss[];
    private Calendar calendario;
    String horas, minutos;
    int month;
    String dia, mes, anno, fechaCon;
    
    @RequestMapping(value = "/login", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody usuarioLogin loginInXML(@RequestBody usuarioLogin entrada) {
            
                System.out.println("hola login");
                
                dao = new usuarioDAOImpl();
                
                obj = new Usuario();
                
                if (entrada.getClave().isEmpty()){
                   entrada.setNickname("ERROR 004");
                   entrada.setClave("ERROR 004: Clave nulo");
                   entrada.setToken("ERROR 004");
                  
                   return entrada;
               }    
                else
                    if (entrada.getNickname().isEmpty()){
                        entrada.setNickname("ERROR 001: Nickname nulo");
                        entrada.setClave("ERROR 001");
                        entrada.setToken("ERROR 001");
                  
                   return entrada;
               }
               
               System.out.println("nickName: "+entrada.getNickname()); 
               obj = dao.findUsuario(entrada.getNickname());
               //System.out.println("nickName de obj: "+obj.getNickname());
//               System.out.println(obj.getNickname().toString());
               if (obj.getNickname().isEmpty()) {
                   
                   System.out.println("El nickName introducido no existe");
                        entrada.setNickname("ERROR 001: Nickname no existe");
                        entrada.setClave("ERROR 001");
                        entrada.setToken("ERROR 001");
                   return entrada;
                   
               }
               else
                   System.out.println("El nickName si existe: "+obj.getNickname());
               
               if (!entrada.getClave().equals(obj.getClave())){
                   
                   System.out.println("La clave introducida es incorrecta");
                        entrada.setNickname("ERROR 003");
                        entrada.setClave("ERROR 003: Clave incorrecta");
                        entrada.setToken("ERROR 003");
                   return entrada;
                   
               }
               else
                   System.out.println("La clave es correcta: "+obj.getClave());
               
               Tokken findTokenViejo = new Tokken();
               
               daoTok = new tokkenDAOImpl();
               
               findTokenViejo = daoTok.findTokken(entrada.getNickname());
               
               if (!findTokenViejo.getTokken().isEmpty()) {
                   
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
                        
                        if (total >= 3){   //AQUI CAMBIAR PARA LA DURACION DE LOS MINUTOS
                            
                            Tokken tok = new Tokken();
                
                            daoTok = new tokkenDAOImpl();
               
                            Random ramdom = new Random();
               
                            int numero = ramdom.nextInt(999999999);
                            String num = String.valueOf(numero);
               
                            System.out.println("Numero aleatorio: "+numero);
               
                            entrada.setToken(num);
               
                            tok.setTokken(num);

                            String remoteAddress = ((ServletRequestAttributes)RequestContextHolder.currentRequestAttributes()).getRequest().getRemoteAddr();
                //                System.out.println("IP: "+remoteAddress);

                                tok.setIp(remoteAddress);

                                calendario = Calendar.getInstance();

                                horas = Integer.toString(calendario.get(Calendar.HOUR_OF_DAY));
                                minutos = Integer.toString(calendario.get(Calendar.MINUTE));
                                dia = Integer.toString(calendario.get(Calendar.DATE));
                                month = calendario.get(Calendar.MONTH);
                                month = month + 1;
                                mes = Integer.toString(month);
                                anno = Integer.toString(calendario.get(Calendar.YEAR));

                                fechaCon = dia+"/"+mes+"/"+anno+","+horas+":"+minutos;
                                tok.setFecha_creacion(fechaCon);
                                tok.setUsuario(entrada.getNickname());

                                System.out.println("IP: "+remoteAddress);
                                System.out.println("Fecha: "+fechaCon);

                                daoTok.insertTokken(tok);



                                return entrada;
                            
                        }
                        else {
                            
                            System.out.println("Token vigente");
                            entrada.setNickname("ERROR 100");
                            entrada.setClave("ERROR 100");
                            entrada.setToken("ERROR 100: Token vigente");
                            return entrada;
                            
                        }
                        
                    }
                   
               }
               
               Tokken tok = new Tokken();
                
               daoTok = new tokkenDAOImpl();
               
               Random ramdom = new Random();
               
               int numero = ramdom.nextInt(999999999);
               String num = String.valueOf(numero);
               
               System.out.println("Numero aleatorio: "+numero);
               
               entrada.setToken(num);
               
               tok.setTokken(num);
               
               String remoteAddress = ((ServletRequestAttributes)RequestContextHolder.currentRequestAttributes()).getRequest().getRemoteAddr();
//                System.out.println("IP: "+remoteAddress);
                
                tok.setIp(remoteAddress);
                
                calendario = Calendar.getInstance();
                
                horas = Integer.toString(calendario.get(Calendar.HOUR_OF_DAY));
                minutos = Integer.toString(calendario.get(Calendar.MINUTE));
                dia = Integer.toString(calendario.get(Calendar.DATE));
                month = calendario.get(Calendar.MONTH);
                month = month + 1;
                mes = Integer.toString(month);
                anno = Integer.toString(calendario.get(Calendar.YEAR));
                
                fechaCon = dia+"/"+mes+"/"+anno+","+horas+":"+minutos;
                tok.setFecha_creacion(fechaCon);
                tok.setUsuario(entrada.getNickname());
                
                System.out.println("IP: "+remoteAddress);
                System.out.println("Fecha: "+fechaCon);
                
                daoTok.insertTokken(tok);
               
               
                
                return entrada;
                
        }
    
}
