/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

/**
 *
 * @author Antonio
 */
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import prueba.DAO.authorDAO;
import prueba.DAO.authorDAOImpl;
import prueba.modelo.author;


@Controller
@RequestMapping("/buscar")
public class buscarAuthor {
    
    private authorDAO dao;
    
    	@RequestMapping(value="{name}", method = RequestMethod.GET)
	public @ResponseBody author getAuthorInXML(@PathVariable String name) {
            
                System.out.println("holaaaa");
            
		author autor = new author();
                
                dao = new authorDAOImpl();
                
                System.out.println("holaaaa2");
                System.out.println(name);
                autor = dao.findAuthor(name);
                
                System.out.println("hola3");
		
		return autor;

	}
    
}
