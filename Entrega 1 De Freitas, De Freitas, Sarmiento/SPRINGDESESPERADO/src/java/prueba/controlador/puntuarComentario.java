/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.controlador;

import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import prueba.DAO.comentarioDAO;
import prueba.DAO.comentarioDAOImpl;
import prueba.DAO.usuarioDAO;
import prueba.DAO.usuarioDAOImpl;
import prueba.modelo.Comentario;
import prueba.modelo.Usuario;
import prueba.modelo.listaComentarios;

/**
 *
 * @author Antonio
 */

@Controller
public class puntuarComentario {
    
    private comentarioDAO dao;
    
    @RequestMapping(value = "/comentario/puntuar", method = RequestMethod.POST, consumes = "application/xml", produces="application/xml")
        
    	public @ResponseBody Comentario puntuarComentarioInXML(@RequestBody Comentario entrada) {
            
                System.out.println("hola puntuarComentario");

//                StreamSource source = new StreamSource(new StringReader(entrada));
//                Jaxb2Marshaller prueba = new Jaxb2Marshaller();
                System.out.println(entrada.toString());
//                Usuario e = (Usuario) prueba.unmarshal(source);
//                System.out.println(entrada.toString());
                //System.out.println(jaxb2Marshaller.unmarshal(source));
//                Usuario e = (Usuario) jaxb2Marshaller.unmarshal(source);

		Comentario comment = new Comentario();
                
                dao = new comentarioDAOImpl();
                
                comment = dao.findComentario(entrada.getId());
                
                dao.insertarComentarioPuntuacionSuperColumn(entrada);
                
//                System.out.println("Fecha: "+ entrada.getFecha_creacion());
//                System.out.println("Adjunto: "+ entrada.getAdjunto());                
//                System.out.println("Contenido: "+ entrada.getmensaje());
//                System.out.println("Nickname: "+ entrada.getnickName());
//                 
//                String idD = dao.getID();
//                System.out.println("ID mas grande de la base de datos: "+idD);
//                
//                int codigoID;
//                codigoID = Integer.parseInt(idD);
//                codigoID = codigoID + 1;
//                
//                idD = String.valueOf(codigoID);
//                System.out.println("ID a enviar al dao: "+idD);
//                entrada.setId(idD);
//                entrada.setReply("0");
//                

//                dao.insertComentario(entrada);
//                dao.insertarComentarioSuperColumn(entrada);

                //user = dao.findUsuario(e.getNickname());                
		
		return comment;

	}
    
}
