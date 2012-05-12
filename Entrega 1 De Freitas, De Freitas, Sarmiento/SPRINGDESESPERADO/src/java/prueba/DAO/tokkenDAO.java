/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.DAO;

import java.util.Collection;
import prueba.modelo.Tokken;

/**
 *
 * @author Antonio
 */
public interface tokkenDAO {
    
    public boolean insertTokken(Tokken seguridad);
    
    public Tokken findTokken(String nickname);
    
}
