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
import prueba.DAO.*;
import prueba.modelo.Etiqueta;
import prueba.modelo.Usuario;
import prueba.modelo.listaEtiquetas;
import prueba.modelo.listaUsuarios;


@Controller
public class todosEtiquetas {
    
        private etiquetaDAO dao;
    
        @RequestMapping(value="/etiquetas", method=RequestMethod.GET)
        public listaEtiquetas getUsuarios() {
            
                System.out.println("Hola todosEtiquetas");
                dao = new etiquetaDAOImpl();
                List<Etiqueta> etiquetas = (List<Etiqueta>) dao.listEtiqueta();
                listaEtiquetas list = new listaEtiquetas(etiquetas);
                System.out.println("Chao todosEtiquetas");
                
        return list;               
        }
    
}
