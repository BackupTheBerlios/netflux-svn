/* 
 * netflux-core - Copyright (C) 2005 OPEN input - http://www.openinput.com/
 *
 * This program is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU General Public License as published by the 
 * Free Software Foundation; either version 2 of the License, or (at your 
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. 
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along 
 * with this program; if not, write to 
 *   the Free Software Foundation, Inc., 
 *   59 Temple Place, Suite 330, 
 *   Boston, MA 02111-1307 USA
 *   
 * $Id$
 */
package org.netflux.core.task.filter;

import org.apache.bsf.BSFEngine;
import org.apache.bsf.BSFException;
import org.apache.bsf.BSFManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.Field;
import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;

/**
 * <p>
 * Scripting language based filter using the <a href="http://jakarta.apache.org/bsf/">Bean Scripting Framework</a>. Thanks to the use
 * of <a href="http://jakarta.apache.org/bsf/">BSF</a> you may use this class to implement a filter using one of the many BSF
 * supported scripting languages (Java, Javascript, Python, Groovy,...).
 * </p>
 * <p>
 * In order to use this filter, you must provide an scripting language to use and an expression written in that language that will be
 * evaluated for every record. The result of the evaluation will determine acceptance or rejection of the corresponding record. The
 * {@link org.netflux.core.Field field}s of the record will be made available to the scripting engine, so they may be accessed in the
 * following way: <code>nameOfField.value</code>.
 * </p>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class BSFFilter implements Filter
  {
  private static Log log = LogFactory.getLog( BSFFilter.class );

  private String     language;
  private String     expression;

  /**
   * Returns the name of the scripting language used by this filter.
   * 
   * @return the name of the scripting language used by this filter.
   */
  public String getLanguage( )
    {
    return this.language;
    }

  /**
   * Sets the scripting language used by this filter. Please, refer to the <a href="http://jakarta.apache.org/bsf/">BSF</a>
   * documentation to find about valid scripting language names.
   * 
   * @param language the name of the scripting language to be used by this filter.
   */
  public void setLanguage( String language )
    {
    this.language = language;
    }

  /**
   * Returns the expression to be evaluated to filter records.
   * 
   * @return the expression to be evaluated to filter records.
   */
  public String getExpression( )
    {
    return this.expression;
    }

  /**
   * Sets the expressión to be evaluated to filter records. Inside this expression, a typical object oriented scripting language like
   * <a href="http://www.beanshell.org/">BeanShell</a> may access field values using an expression like this:
   * <code>nameOfField.value</code>. The expression should return a boolean value, or at least a value that can be casted to
   * boolean.
   * 
   * @param expression the expression to be used to filter records.
   */
  public void setExpression( String expression )
    {
    this.expression = expression;
    }

  public boolean accepts( Record record )
    {
    try
      {
      BSFManager manager = new BSFManager( );
      for( FieldMetadata fieldMetadata : record.getMetadata( ).getFieldMetadata( ) )
        {
        String fieldName = fieldMetadata.getName( );
        manager.declareBean( fieldName, record.getField( fieldName ), Field.class );
        }

      BSFEngine engine = manager.loadScriptingEngine( this.getLanguage( ) );
      return ((Boolean) engine.eval( "filter", 0, 0, this.getExpression( ) )).booleanValue( );
      }
    catch( BSFException exc )
      {
      BSFFilter.log.debug( "Exception while evaluating expression", exc );
      return false;
      }
    }
  }
