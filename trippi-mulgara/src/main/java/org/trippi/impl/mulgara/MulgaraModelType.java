package org.trippi.impl.mulgara;

import java.net.URI;
import java.net.URISyntaxException;

import org.mulgara.query.rdf.Mulgara;

/**
 * Enum type that represents Mulgara model types.<br>
 * The subset of Mulgara's optional model types represented by this enum are:
 * <ul>
 * 	<li>http://mulgara.org/mulgara#Model<br>
 *		This is the default triple store model. Specifying this type is 
 *		equivalent to omitting the type parameter.
 *	<li>http://mulgara.org/mulgara#LuceneModel<br>
 *		A full-text string index model based on Lucene.
 *	<li>http://mulgara.org/mulgara#XMLSchemaModel<br>
 *		A datatyping model that represents the property of orderedness that 
 *		literals like dates and numbers possess.
 * </ul>
 * 
 * @author Edwin Shin
 * @see <a href="http://docs.mulgara.org/itqlcommands/create.html">iTQL create</a>
 * 
 */
public enum MulgaraModelType {
	MODEL("Model"), XSD("XMLSchemaModel"), LUCENE("LuceneModel");

	private final URI uri;

	MulgaraModelType(String type) {
		try {
			this.uri = new URI(Mulgara.NAMESPACE + type);
		} catch (URISyntaxException e) {
			throw new Error("Bad hardcoded URI");
		}
	}
	
	/**
	 * 
	 * @return the <code>URI</code> representing the Mulgara model type.
	 */
	URI uri() {
		return uri;
	}
}
