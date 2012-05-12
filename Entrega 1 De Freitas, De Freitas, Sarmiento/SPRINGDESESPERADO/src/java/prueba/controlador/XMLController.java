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
import prueba.modelo.Coffee;


@Controller
@RequestMapping("/coffee")
public class XMLController {

	@RequestMapping(value="{name}", method = RequestMethod.GET)
	public @ResponseBody Coffee getCoffeeInXML(@PathVariable String name) {

		Coffee coffee = new Coffee(name, 100, "yo");
		
		return coffee;

	}
	
}
