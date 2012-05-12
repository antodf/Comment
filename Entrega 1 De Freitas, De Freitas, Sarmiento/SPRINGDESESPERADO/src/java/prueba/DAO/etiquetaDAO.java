/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.DAO;

import java.util.Collection;
import prueba.modelo.Etiqueta;

/**
 *
 * @author Antonio
 */
public interface etiquetaDAO {
    
    public boolean insertEtiqueta(Etiqueta tag);
    
    public boolean deleteEtiqueta(String nombre);
    
    public Etiqueta findEtiqueta(String nombre);
    
    public Collection<Etiqueta> listEtiqueta();
    
    public int countEtiqueta();
    
}
