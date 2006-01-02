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
package org.netflux.core.task.transform;

import java.io.Serializable;
import java.util.Collections;

import org.apache.bsf.BSFEngine;
import org.apache.bsf.BSFException;
import org.apache.bsf.BSFManager;
import org.netflux.core.Field;
import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class BSFExpresionTransformation implements Transformation
  {
  private String         language;
  private String         expression;
  private FieldMetadata  outputFieldMetadata;
  private RecordMetadata outputMetadata;

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

  /**
   * @return Returns the outputFieldMetadata.
   */
  public FieldMetadata getOutputFieldMetadata( )
    {
    return this.outputFieldMetadata;
    }

  /**
   * @param outputFieldMetadata The outputFieldMetadata to set.
   */
  public void setOutputFieldMetadata( FieldMetadata outputFieldMetadata )
    {
    this.outputFieldMetadata = outputFieldMetadata;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#setInputMedatata(org.netflux.core.RecordMetadata)
   */
  public void setInputMedatata( RecordMetadata metadata )
    {
    // Nothing really useful to be done here
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#getOutputMetadata()
   */
  public RecordMetadata getOutputMetadata( )
    {
    if( this.outputMetadata == null )
      {
      this.outputMetadata = new RecordMetadata( Collections.singletonList( this.outputFieldMetadata ) );
      }

    return this.outputMetadata;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#transform(org.netflux.core.Record)
   */
  public Record transform( Record record )
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
      Object result = engine.eval( "expression", 0, 0, this.getExpression( ) );
      Record transformedRecord = new Record( this.getOutputMetadata( ) );
      transformedRecord.setValue( this.getOutputFieldMetadata( ).getName( ), (Serializable) result );
      return transformedRecord;
      }
    catch( BSFException exc )
      {
      exc.printStackTrace( );
      return null;
      }
    }
  }
