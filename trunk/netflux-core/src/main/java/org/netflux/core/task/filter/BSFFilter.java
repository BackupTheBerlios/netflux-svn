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
import org.netflux.core.Field;
import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class BSFFilter implements Filter
  {
  private String language;
  private String expression;

  /**
   * @return Returns the language.
   */
  public String getLanguage( )
    {
    return this.language;
    }

  /**
   * @param language The language to set.
   */
  public void setLanguage( String language )
    {
    this.language = language;
    }

  /**
   * @return Returns the expression.
   */
  public String getExpression( )
    {
    return this.expression;
    }

  /**
   * @param expression The expression to set.
   */
  public void setExpression( String expression )
    {
    this.expression = expression;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.filter.Filter#accepts(org.netflux.core.Record)
   */
  public boolean accepts( Record record )
    {
    // TODO: Add exception handling
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
      return false;
      }
    }
  }
