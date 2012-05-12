/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package prueba.modelo;

/**
 *
 * @author Antonio
 */
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "coffee")
public class Coffee {

	String name;
	int quanlity;
        String pass;

	public String getName() {
		return name;
	}

	@XmlElement
	public void setName(String name) {
		this.name = name;
	}

	public int getQuanlity() {
		return quanlity;
	}

	@XmlElement
	public void setQuanlity(int quanlity) {
		this.quanlity = quanlity;
	}
        
        public String getPass() {
		return pass;
	}

	@XmlElement
	public void setPass(String name) {
		this.pass = pass;
	}

	public Coffee(String name, int quanlity, String pass) {
		this.name = name;
		this.quanlity = quanlity;
                this.pass = pass;
	}

	public Coffee() {
		super();
	}

	
	
}
