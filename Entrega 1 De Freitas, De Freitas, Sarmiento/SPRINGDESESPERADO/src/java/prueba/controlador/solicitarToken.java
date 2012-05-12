/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

/**
 *
 * @author Antonio
 */

import java.io.StringReader;
import java.util.Calendar;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.Marshaller;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import prueba.DAO.*;
import prueba.modelo.Tokken;
import prueba.modelo.Usuario;

@Controller
public class solicitarToken {
    
    private tokkenDAO dao;
    
    @RequestMapping(value = "/insertarToken", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody Tokken insertarTokenInXML(@RequestBody Tokken entrada) {
            
                System.out.println("hola InsertarToken");

//                StreamSource source = new StreamSource(new StringReader(entrada));
//                Jaxb2Marshaller prueba = new Jaxb2Marshaller();
                System.out.println(entrada.toString());
//                Usuario e = (Usuario) prueba.unmarshal(source);
//                System.out.println(entrada.toString());
                //System.out.println(jaxb2Marshaller.unmarshal(source));
//                Usuario e = (Usuario) jaxb2Marshaller.unmarshal(source);
                
		Tokken tok = new Tokken();
                
                dao = new tokkenDAOImpl();
//                System.out.println("Nickname: "+ entrada.getUsuario());
                
                entrada.setTokken("Token");
                
                String remoteAddress = ((ServletRequestAttributes)RequestContextHolder.currentRequestAttributes()).getRequest().getRemoteAddr();
//                System.out.println("IP: "+remoteAddress);
                
                entrada.setIp(remoteAddress);
                
                Calendar calendario = Calendar.getInstance();
                String horas, minutos;
                int month;
                String dia, mes, anno, fechaCon;
                
                horas = Integer.toString(calendario.get(Calendar.HOUR_OF_DAY));
                minutos = Integer.toString(calendario.get(Calendar.MINUTE));
                dia = Integer.toString(calendario.get(Calendar.DATE));
                month = calendario.get(Calendar.MONTH);
                month = month + 1;
                mes = Integer.toString(month);
                anno = Integer.toString(calendario.get(Calendar.YEAR));
                
                fechaCon = dia+"/"+mes+"/"+anno+","+horas+":"+minutos;
                entrada.setFecha_creacion(fechaCon);
                
                System.out.println("Nickname: "+ entrada.getUsuario());
                System.out.println("IP: "+remoteAddress);
                System.out.println("Fecha: "+fechaCon);
                
                dao.insertTokken(entrada);

                //user = dao.findUsuario(e.getNickname());                
		
		return entrada;

	}
    
}
