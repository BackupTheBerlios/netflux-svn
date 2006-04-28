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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

// TODO: Type checking???????
/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class LookupTransformation implements Transformation
  {
  public enum MissingBehaviour
    {
    DEFAULT_VALUE, PASS_THROUGH, THROW_EXCEPTION
    }

  protected String                                inputFieldName;
  protected FieldMetadata                         outputFieldMetadata;
  protected LookupTableFactory                    lookupTableFactory;
  protected LookupTransformation.MissingBehaviour missingBehaviour = LookupTransformation.MissingBehaviour.DEFAULT_VALUE;
  protected Serializable                          defaultValue     = null;
  protected RecordMetadata                        inputMetadata    = new RecordMetadata( );
  protected RecordMetadata                        outputMetadata   = new RecordMetadata( );

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

  /**
   * @return Returns the missingBehaviour.
   */
  public LookupTransformation.MissingBehaviour getMissingBehaviour( )
    {
    return this.missingBehaviour;
    }

  /**
   * @param missingBehaviour The missingBehaviour to set.
   */
  public void setMissingBehaviour( LookupTransformation.MissingBehaviour missingBehaviour )
    {
    this.missingBehaviour = missingBehaviour;
    }

  /**
   * @return Returns the defaultValue.
   */
  public Serializable getDefaultValue( )
    {
    return this.defaultValue;
    }

  /**
   * @param defaultValue The defaultValue to set.
   */
  public void setDefaultValue( Serializable defaultValue )
    {
    this.defaultValue = defaultValue;
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
    Serializable valueToTransform = record.getValue( Serializable.class, this.inputFieldName );
    Map<?, ? extends Serializable> lookupTable = this.getLookupTableFactory( ).getLookupTable( );

    if( lookupTable.containsKey( valueToTransform ) )
      {
      Serializable transformedValue = lookupTable.get( valueToTransform );
      transformedRecord.setValue( this.outputFieldMetadata.getName( ), transformedValue );
      }
    else if( valueToTransform == null )
      {
      transformedRecord.setValue( this.outputFieldMetadata.getName( ), valueToTransform );
      }
    else
      {
      switch( this.getMissingBehaviour( ) )
        {
        case DEFAULT_VALUE:
          transformedRecord.setValue( this.outputFieldMetadata.getName( ), this.getDefaultValue( ) );
          break;
        case PASS_THROUGH:
          if( this.inputMetadata.getFieldMetadata( this.getInputFieldName( ) ).getType( ) == this.getOutputFieldMetadata( ).getType( ) )
            {
            transformedRecord.setValue( this.outputFieldMetadata.getName( ), valueToTransform );
            }
          else
            {
            // FIXME: Throw a big exception... maybe we shouldn't allow PASS_THROUGH if input and output metadata are different...
            transformedRecord.setValue( this.outputFieldMetadata.getName( ), this.getDefaultValue( ) );
            }
          break;
        case THROW_EXCEPTION:
          // FIXME: Throw a big exception...
          transformedRecord.setValue( this.outputFieldMetadata.getName( ), this.getDefaultValue( ) );
          break;
        }
      }

    return transformedRecord;
    }
  }
