/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.DAO;

import java.util.Collection;
import prueba.modelo.author;

/**
 *
 * @author Antonio
 */
public interface authorDAO {
    
	public boolean insertAuthor(author user);

	public boolean deleteAuthor(String username);

	public author findAuthor(String username);

	public boolean updateAuthor(author user);

	public Collection<author> listAuthor();
	
	public int countAuthor();
    
}
