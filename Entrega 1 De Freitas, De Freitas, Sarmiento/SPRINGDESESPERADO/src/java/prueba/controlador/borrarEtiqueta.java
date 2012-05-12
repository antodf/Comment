/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import prueba.DAO.*;
import prueba.modelo.Etiqueta;
import prueba.modelo.Usuario;

/**
 *
 * @author Antonio
 */

@Controller
@RequestMapping("/borrar/etiqueta")
public class borrarEtiqueta {
    
    private etiquetaDAO dao;
    
    	@RequestMapping(value="{name}", method = RequestMethod.DELETE)
	public @ResponseBody Etiqueta borrarEtiqueta(@PathVariable String name) {
            
                System.out.println("holaaaa");
            
		Etiqueta tag = new Etiqueta();
                
                dao = new etiquetaDAOImpl();
                
                System.out.println("holaaaa2");
                System.out.println(name);
                
                tag = dao.findEtiqueta(name);
                //if (tag.getNombre() != null)
                dao.deleteEtiqueta(name);
                
                System.out.println("hola3");
		
		return tag;

	}
    
}
