/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.modelo;

import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 *
 * @author Antonio
 */

@XmlRootElement(name="usuarios")
public class listaUsuarios {
    
    	private int count;
	private List<Usuario> usuarios;

	public listaUsuarios() {}
	
	public listaUsuarios(List<Usuario> usuarios) {
		this.usuarios = usuarios;
		this.count = usuarios.size();
	}

	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	@XmlElement(name="usuario")
	public List<Usuario> getEmployees() {
		return usuarios;
	}
	public void setEmployees(List<Usuario> employees) {
		this.usuarios = employees;
	}
    
}
