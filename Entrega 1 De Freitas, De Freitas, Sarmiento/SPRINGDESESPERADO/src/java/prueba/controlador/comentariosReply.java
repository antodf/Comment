/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

/**
 *
 * @author Antonio
 */
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import prueba.DAO.comentarioDAO;
import prueba.DAO.comentarioDAOImpl;
import prueba.DAO.usuarioDAO;
import prueba.DAO.usuarioDAOImpl;
import prueba.modelo.Comentario;
import prueba.modelo.Usuario;
import prueba.modelo.listaComentarios;

@Controller
@RequestMapping("/replyComentarios")
public class comentariosReply {
    
    private comentarioDAO dao;
    String[] arreglo = new String [100];
    
    	@RequestMapping(value="{name}", method = RequestMethod.GET)
	public @ResponseBody listaComentarios getComentariosReplyInXML(@PathVariable String name) {
            
                dao = new comentarioDAOImpl();
                List<Comentario> comentarios = (List<Comentario>) dao.comentariosReply(name);
                listaComentarios list = new listaComentarios(comentarios);
                
                return list;
            
        }
    
}
