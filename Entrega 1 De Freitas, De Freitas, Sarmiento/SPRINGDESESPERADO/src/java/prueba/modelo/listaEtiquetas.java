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

@XmlRootElement(name="etiquetas")
public class listaEtiquetas {
    
    private int count;
	private List<Etiqueta> etiquetas;

	public listaEtiquetas() {}
	
	public listaEtiquetas(List<Etiqueta> etiquetas) {
		this.etiquetas = etiquetas;
		this.count = etiquetas.size();
	}

	public int getCount() {
		return count;
	}
	public void setCount(int contador) {
		this.count = contador;
	}
	
	@XmlElement(name="etiqueta")
	public List<Etiqueta> getEtiquetas() {
		return etiquetas;
	}
	public void setEtiquetas(List<Etiqueta> etiquetasss) {
		this.etiquetas = etiquetasss;
	}
    
}
