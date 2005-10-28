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

import java.util.LinkedList;
import java.util.List;

import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

// TODO: Type checking???????
/**
 * @author jgonzalez
 */
public class LookupTransformation implements Transformation
  {
  protected String             inputFieldName;
  protected FieldMetadata      outputFieldMetadata;
  protected LookupTableFactory lookupTableFactory;
  protected RecordMetadata     inputMetadata  = new RecordMetadata( );
  protected RecordMetadata     outputMetadata = new RecordMetadata( );

  /**
   * @return Returns the inputFieldName.
   */
  public String getInputFieldName( )
    {
    return this.inputFieldName;
    }

  /**
   * @param inputFieldName The inputFieldName to set.
   */
  public void setInputFieldName( String inputFieldName )
    {
    this.inputFieldName = inputFieldName;
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

  /**
   * @return Returns the lookupTableFactory.
   */
  public LookupTableFactory getLookupTableFactory( )
    {
    return this.lookupTableFactory;
    }

  /**
   * @param lookupTableFactory The lookupTableFactory to set.
   */
  public void setLookupTableFactory( LookupTableFactory lookupTableFactory )
    {
    this.lookupTableFactory = lookupTableFactory;
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
    if( this.inputFieldName != null && this.outputFieldMetadata != null && this.inputMetadata != null
        && this.inputMetadata.getFieldCount( ) > 0 )
      {
      List<FieldMetadata> newOutputMetadata = new LinkedList<FieldMetadata>( );
      newOutputMetadata.add( this.outputFieldMetadata );

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
    Object transformedValue = this.getLookupTableFactory( ).getLookupTable( )
        .get( record.getValue( Object.class, this.inputFieldName ) );
    transformedRecord.setValue( this.outputFieldMetadata.getName( ), transformedValue );

    return transformedRecord;
    }
  }
