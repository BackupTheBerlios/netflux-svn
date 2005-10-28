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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

/**
 * @author jgonzalez
 */
public class CopyTransformation implements Transformation
  {
  protected List<String>   inputFieldNames  = new LinkedList<String>( );
  protected List<String>   outputFieldNames = new LinkedList<String>( );
  protected RecordMetadata inputMetadata    = new RecordMetadata( );
  protected RecordMetadata outputMetadata   = new RecordMetadata( );

  /**
   * @return Returns the inputFieldNames.
   */
  public List<String> getInputFieldNames( )
    {
    return Collections.unmodifiableList( this.inputFieldNames );
    }

  /**
   * @param inputFieldNames The inputFieldNames to set.
   */
  public void setInputFieldNames( List<String> inputFieldNames )
    {
    this.inputFieldNames = new ArrayList<String>( inputFieldNames );
    this.updateOutputMetadata( );
    }

  /**
   * @return Returns the outputFieldNames.
   */
  public List<String> getOutputFieldNames( )
    {
    return Collections.unmodifiableList( this.outputFieldNames );
    }

  /**
   * @param outputFieldNames The outputFieldNames to set.
   */
  public void setOutputFieldNames( List<String> outputFieldNames )
    {
    this.outputFieldNames = new ArrayList<String>( outputFieldNames );
    this.updateOutputMetadata( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#setInputMedatata(org.netflux.core.RecordMetadata)
   */
  public void setInputMedatata( RecordMetadata metadata )
    {
    this.inputMetadata = metadata;
    this.updateOutputMetadata( );
    }

  /**
   * 
   */
  protected void updateOutputMetadata( )
    {
    if( this.inputFieldNames.size( ) == this.outputFieldNames.size( ) && this.inputMetadata != null
        && this.inputMetadata.getFieldCount( ) > 0 )
      {
      List<FieldMetadata> newOutputMetadata = new LinkedList<FieldMetadata>( );
      Iterator<String> inputFieldsIterator = this.inputFieldNames.iterator( );
      Iterator<String> outputFieldsIterator = this.outputFieldNames.iterator( );
      while( inputFieldsIterator.hasNext( ) )
        {
        // FIXME: getFieldMetadata should throw an Exception
        FieldMetadata currentFieldMetadata = this.inputMetadata.getFieldMetadata( inputFieldsIterator.next( ) );
        if( currentFieldMetadata != null )
          {
          FieldMetadata outputFieldMetadata = currentFieldMetadata.clone( );
          outputFieldMetadata.setName( outputFieldsIterator.next( ) );
          newOutputMetadata.add( outputFieldMetadata );
          }
        else
          {
          newOutputMetadata.clear( );
          break;
          }
        }
      this.outputMetadata = new RecordMetadata( newOutputMetadata );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#getOutputMetadata()
   */
  public RecordMetadata getOutputMetadata( )
    {
    return this.outputMetadata;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#transform(org.netflux.core.Record)
   */
  public Record transform( Record record )
    {
    Record transformedRecord = new Record( this.outputMetadata );

    Iterator<String> inputFieldsIterator = this.inputFieldNames.iterator( );
    Iterator<String> outputFieldsIterator = this.outputFieldNames.iterator( );
    while( inputFieldsIterator.hasNext( ) )
      {
      transformedRecord.setField( outputFieldsIterator.next( ), record.getField( inputFieldsIterator.next( ) ) );
      }

    return transformedRecord;
    }
  }
