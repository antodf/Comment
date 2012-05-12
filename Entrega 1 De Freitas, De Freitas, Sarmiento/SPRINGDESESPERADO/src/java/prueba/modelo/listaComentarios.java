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

@XmlRootElement(name="comentarios")
public class listaComentarios {
    
    private int count;
	private List<Comentario> mensaje;

	public listaComentarios() {}
	
	public listaComentarios(List<Comentario> comentarios) {
		this.mensaje = comentarios;
		this.count = comentarios.size();
	}

	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	@XmlElement(name="comentario")
	public List<Comentario> getComentarios() {
		return mensaje;
	}
	public void setComentarios(List<Comentario> comentarios) {
		this.mensaje = comentarios;
	}
    
}
