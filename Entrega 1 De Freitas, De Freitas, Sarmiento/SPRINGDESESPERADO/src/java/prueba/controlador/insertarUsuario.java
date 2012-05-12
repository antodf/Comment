/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

/**
 *
 * @author Antonio
 */

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
import prueba.DAO.authorDAO;
import prueba.DAO.authorDAOImpl;
import prueba.DAO.usuarioDAO;
import prueba.DAO.usuarioDAOImpl;
import prueba.modelo.Usuario;

@Controller
//@RequestMapping("/insertar")
public class insertarUsuario {
    
	private Jaxb2Marshaller jaxb2Marshaller;
	
	public void setJaxb2Mashaller(Jaxb2Marshaller jaxb2Mashaller) {
		this.jaxb2Marshaller = jaxb2Mashaller;
	}
    
    private usuarioDAO dao;
    
    @RequestMapping(value = "/insertarUsuario", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody Usuario insertarUsuarioInXML(@RequestBody Usuario entrada) {
            
                System.out.println("hola InsertarUsuario");

//                StreamSource source = new StreamSource(new StringReader(entrada));
//                Jaxb2Marshaller prueba = new Jaxb2Marshaller();
                System.out.println(entrada.toString());
//                Usuario e = (Usuario) prueba.unmarshal(source);
//                System.out.println(entrada.toString());
                //System.out.println(jaxb2Marshaller.unmarshal(source));
//                Usuario e = (Usuario) jaxb2Marshaller.unmarshal(source);

		Usuario user = new Usuario();
                
                dao = new usuarioDAOImpl();
                System.out.println("Nickname: "+ entrada.getNickname());
                
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
               
                dao.insertUsuario(entrada);
         //System.out.println("sali del if");

                //user = dao.findUsuario(e.getNickname());                
		
		return entrada;

	}
    
}
