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

/**
 *
 * @author Antonio
 */

@Controller
public class updateComentario {
    
    private comentarioDAO dao;
    
    @RequestMapping(value = "/updateComentario", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody Comentario updateComentarioInXML(@RequestBody Comentario entrada) {
            
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
                //System.out.println("Nickname: "+ entrada.getnickName());
                 
                
                dao.insertComentario(entrada);

                //user = dao.findUsuario(e.getNickname());                
		
		return entrada;

	}
    
}
