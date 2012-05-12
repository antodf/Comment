/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.DAO;

import java.util.Collection;
import prueba.modelo.Comentario;
import prueba.modelo.Usuario;
/**
 *
 * @author Antonio
 */
public interface usuarioDAO {
    
    	public boolean insertUsuario(Usuario user);

	public boolean deleteUsuario(String nickname);

	public Usuario findUsuario(String nickname);

	public boolean updateUsuario(Usuario user);

	public Collection<Usuario> listUsuario();
	
	public int countUsuario();
        
        public Collection<Comentario> comentariosUsuario(String ruta);
        
        public void borrarUsuarioSuperColumn(String name);
    
}
