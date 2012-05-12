/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

/**
 *
 * @author Antonio
 */

import java.util.List;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import prueba.DAO.authorDAO;
import prueba.DAO.authorDAOImpl;
import prueba.DAO.usuarioDAO;
import prueba.DAO.usuarioDAOImpl;
import prueba.modelo.Usuario;
import prueba.modelo.listaUsuarios;

@Controller
//@RequestMapping("/buscarUsuario")
public class todosUsuarios {
    
    private usuarioDAO dao;
    
        @RequestMapping(value="/usuarios", method=RequestMethod.GET)
        public listaUsuarios getUsuarios() {
            
                dao = new usuarioDAOImpl();
                List<Usuario> usuarios = (List<Usuario>) dao.listUsuario();
                listaUsuarios list = new listaUsuarios(usuarios);
                
        return list;               
        }
    
}
