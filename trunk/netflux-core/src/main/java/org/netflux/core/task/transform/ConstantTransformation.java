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

import org.netflux.core.Field;
import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

// TODO: Type checking???????
/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class ConstantTransformation<T extends Serializable> implements Transformation
  {
  protected FieldMetadata outputFieldMetadata;
  protected Field<T>      outputField;
  protected Record        outputRecord;

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
    this.updateOutputRecord( );
    }

  /**
   * @return Returns the outputField.
   */
  public Field<T> getOutputField( )
    {
    return this.outputField;
    }

  /**
   * @param outputField The outputField to set.
   */
  public void setOutputField( Field<T> outputField )
    {
    this.outputField = outputField;
    this.updateOutputRecord( );
    }

  protected void updateOutputRecord( )
    {
    if( this.getOutputFieldMetadata( ) != null && this.getOutputField( ) != null )
      {
      this.outputRecord = new Record( new RecordMetadata( Collections.singletonList( this.getOutputFieldMetadata( ) ) ) );
      this.outputRecord.setField( this.getOutputFieldMetadata( ).getName( ), this.getOutputField( ) );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#setInputMedatata(org.netflux.core.RecordMetadata)
   */
  public void setInputMedatata( RecordMetadata metadata )
    {}

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#getOutputMetadata()
   */
  public RecordMetadata getOutputMetadata( )
    {
    return new RecordMetadata( Collections.singletonList( this.getOutputFieldMetadata( ) ) );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#transform(org.netflux.core.Record)
   */
  public Record transform( Record record )
    {
    return (Record) this.outputRecord.clone( );
    }
  }
