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

@XmlRootElement(name="puntuaciones")
public class listaPuntuacion {
    
    private int count;
	private List<Etiqueta> puntuaciones;

	public listaPuntuacion() {}
	
	public listaPuntuacion(List<Etiqueta> punto) {
		this.puntuaciones = punto;
		this.count = punto.size();
	}

	public int getCount() {
		return count;
	}
	public void setCount(int contador) {
		this.count = contador;
	}
	
	@XmlElement(name="puntuacion")
	public List<Etiqueta> getPuntuacion() {
		return puntuaciones;
	}
	public void setPuntuacion(List<Etiqueta> puntos) {
		this.puntuaciones = puntos;
	}
    
}
