/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

import java.util.List;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import prueba.DAO.*;
import prueba.modelo.Comentario;


/**
 *
 * @author Antonio
 */

@Controller
@RequestMapping("/buscarComentario")
public class buscarComentario {
    
    private comentarioDAO dao;
    int identificador;
    
    	@RequestMapping(value="{name}", method = RequestMethod.GET)
	public @ResponseBody Comentario getComentarioInXML(@PathVariable String name) {
            
                System.out.println("holaaaa");
            
		Comentario comment = new Comentario();
                
                dao = new comentarioDAOImpl();
                
                System.out.println("holaaaa2");
                System.out.println(name);
                comment = dao.findComentario(name);
                System.out.println("Comentario raiz: "+comment.getReply());
                
                System.out.println("hola3");
                
//                System.out.println(comment.getId());
//                System.out.println(comment.getmensaje());
//                System.out.println(comment.getFecha_creacion());
//                System.out.println(comment.getAdjunto());
		
		return comment;

	}
    
}
