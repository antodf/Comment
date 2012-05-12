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
import prueba.modelo.listaUsuarios;

@Controller
@RequestMapping("/buscarEtiqueta")
public class buscarEtiqueta {
    
    private etiquetaDAO dao;
    
    	@RequestMapping(value="{name}", method = RequestMethod.GET)
	public @ResponseBody Etiqueta getEtiquetaInXML(@PathVariable String name) {
            
                System.out.println("holaaaa");
            
		Etiqueta tag = new Etiqueta();
                
                dao = new etiquetaDAOImpl();
                
                System.out.println("holaaaa2");
                System.out.println(name);
                tag = dao.findEtiqueta(name);
                
                System.out.println("hola3");
		
		return tag;

	}
    
}
