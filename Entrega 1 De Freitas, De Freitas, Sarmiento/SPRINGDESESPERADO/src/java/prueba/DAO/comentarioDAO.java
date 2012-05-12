/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.DAO;

import java.util.Collection;
import prueba.modelo.Comentario;
import prueba.modelo.Etiqueta;

/**
 *
 * @author Antonio
 */
public interface comentarioDAO {
    
        public boolean insertComentario(Comentario comentario);

	public boolean deleteComentario(String id);

	public Comentario findComentario(String id);

	public boolean updateComentario(Comentario comentario);

	public Collection<Comentario> listComentario();
        
        public String getID();
	
	public int countComentario();
        
        public void insertarComentarioSuperColumn(Comentario comment);
        
        public void borrarComentarioSuperColumn(Comentario comment);
        
        public Collection<Comentario> comentariosReply(String ruta);
        
        public void insertarReplySuperColumn(Comentario comment);
        
        public void borrarReplyRaizSuperColumn(String name);
        
        public void borrarReplySegundoSuperColumn(Comentario comment);
        
        public void deleteComentarioEtiqueta(String ruta);
        
        public void insertarComentarioEtiquetaSuperColumn(Etiqueta tag);
        
        public Collection<Etiqueta> comentarioEtiquetas(String ruta);
        
        public void insertarComentarioPuntuacionSuperColumn(Comentario comment);
        
        public void deleteComentarioPuntuacion(String ruta);
        
        public Collection<Etiqueta> comentarioPuntuacionMeGusta(String ruta);
        
        public Collection<Etiqueta> comentarioPuntuacionNoMeGusta(String ruta);
    
}
