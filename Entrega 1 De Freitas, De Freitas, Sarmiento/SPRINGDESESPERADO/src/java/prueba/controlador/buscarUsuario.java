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
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.spi.http.HttpExchange;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import prueba.DAO.authorDAO;
import prueba.DAO.authorDAOImpl;
import prueba.DAO.usuarioDAO;
import prueba.DAO.usuarioDAOImpl;
import prueba.modelo.Usuario;
import prueba.modelo.listaUsuarios;
import java.util.Calendar;

@Controller
@RequestMapping("/buscarUsuario")
public class buscarUsuario {
    
        private usuarioDAO dao;
    
    	@RequestMapping(value="{name}", method = RequestMethod.GET)
	public @ResponseBody Usuario getUsuarioInXML(@PathVariable String name) {
            
                System.out.println("holaaaa");
            
		Usuario user = new Usuario();
                
                dao = new usuarioDAOImpl();
                
                Calendar calendario = Calendar.getInstance();
                int horas, minutos;
                
                String dia, mes, anno;
                
                horas = calendario.get(Calendar.HOUR_OF_DAY);
                minutos = calendario.get(Calendar.MINUTE);
                dia = Integer.toString(calendario.get(Calendar.DATE));
                mes = Integer.toString(calendario.get(Calendar.MONTH));
                anno = Integer.toString(calendario.get(Calendar.YEAR));
                
                System.out.println("MES: "+calendario.get(Calendar.MONTH));
                System.out.println(horas+" : "+minutos);
                System.out.println(dia+"/"+mes+"/"+anno);
                
//                WebServiceContext wsContext = null;
//                
//                MessageContext mc = wsContext.getMessageContext();
//                HttpExchange exchange = (HttpExchange)mc.get("com.sun.xml.internal.ws.http.exchange");
//                System.out.print("IP: "+exchange.getRemoteAddress().getAddress().getHostAddress());
                
String remoteAddress = ((ServletRequestAttributes)RequestContextHolder.currentRequestAttributes()).getRequest().getRemoteAddr();
            System.out.println("IP: "+remoteAddress);
                
                System.out.println("holaaaa2");
                System.out.println(name);
                user = dao.findUsuario(name);
                
                System.out.println("hola3");
		
		return user;

	}
        
//        @RequestMapping(value="/usuarios", method=RequestMethod.GET)
//        public listaUsuarios getEmployees() {
//            
//                dao = new usuarioDAOImpl();
//                List<Usuario> usuarios = (List<Usuario>) dao.listUsuario();
//                listaUsuarios list = new listaUsuarios(usuarios);
//                
//        return list;               
//        }
    
}
