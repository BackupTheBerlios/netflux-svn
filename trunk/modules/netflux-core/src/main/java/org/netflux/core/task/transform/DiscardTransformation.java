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

import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

/**
 * @author jgonzalez
 */
public class DiscardTransformation implements Transformation
  {
  protected CopyTransformation copyTransformation  = new CopyTransformation( );
  protected List<String>       discardedFieldNames = new LinkedList<String>( );
  protected RecordMetadata     inputMetadata       = new RecordMetadata( );

  /**
   * @return Returns the discardedFieldNames.
   */
  public List<String> getDiscardedFieldNames( )
    {
    return this.discardedFieldNames;
    }

  /**
   * @param discardedFieldNames The discardedFieldNames to set.
   */
  public void setDiscardedFieldNames( List<String> discardedFieldNames )
    {
    this.discardedFieldNames = discardedFieldNames;
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
    this.copyTransformation.setInputMedatata( metadata );
    this.updateOutputMetadata( );
    }

  /**
   * 
   */
  protected void updateOutputMetadata( )
    {
    if( this.inputMetadata != null && this.inputMetadata.getFieldCount( ) > 0 )
      {
      List<String> inputFieldNames = this.inputMetadata.getFieldNames( );
      inputFieldNames.removeAll( this.discardedFieldNames );

      this.copyTransformation.setInputFieldNames( inputFieldNames );
      this.copyTransformation.setOutputFieldNames( inputFieldNames );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#getOutputMetadata()
   */
  public RecordMetadata getOutputMetadata( )
    {
    return this.copyTransformation.getOutputMetadata( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.Transformation#transform(org.netflux.core.Record)
   */
  public Record transform( Record record )
    {
    return this.copyTransformation.transform( record );
    }
  }
