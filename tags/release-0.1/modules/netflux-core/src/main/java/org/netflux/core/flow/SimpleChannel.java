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
package org.netflux.core.flow;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.LinkedList;
import java.util.List;

import org.netflux.core.Channel;
import org.netflux.core.InvalidRecordMetadataException;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;

/**
 * A simple channel that directly forwards the records to process to the registered record sinks.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SimpleChannel implements Channel
  {
  protected PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport( this );
  private RecordMetadata          metadata;
  private List<RecordSink>        dataSinks             = new LinkedList<RecordSink>( );

  public RecordMetadata getMetadata( )
    {
    return this.metadata;
    }

  public void setMetadata( RecordMetadata metadata )
    {
    RecordMetadata oldMetadata = this.metadata;
    this.metadata = metadata;
    for( RecordSink recordSink : this.dataSinks )
      {
      recordSink.setMetadata( this.metadata );
      }

    this.propertyChangeSupport.firePropertyChange( "metadata", oldMetadata, this.metadata );
    }

  /**
   * Consumption implementation that just iterates over the list of registered <code>RecordSink</code>s calling its
   * {@link RecordSink#consume(Record)} method with the supplied <code>record</code>.
   */
  public void consume( Record record )
    {
    if( record.getMetadata( ).equals( this.getMetadata( ) ) || record.equals( Record.END_OF_DATA ) )
      {
      for( RecordSink recordSink : this.dataSinks )
        {
        recordSink.consume( record );
        }
      }
    else
      {
      throw new InvalidRecordMetadataException( record, this.getMetadata( ) );
      }
    }

  public void addDataSink( RecordSink dataSink )
    {
    // TODO: We should check that a record sink is taking data from only a record source
    synchronized( dataSinks )
      {
      if( !this.dataSinks.contains( dataSink ) )
        {
        this.dataSinks.add( dataSink );
        dataSink.setMetadata( this.metadata );
        }
      }
    }

  public void removeDataSink( RecordSink dataSink )
    {
    // TODO: We should do something with the record sink metadata
    synchronized( dataSinks )
      {
      this.dataSinks.remove( dataSink );
      }
    }

  public void setDataSinks( List<RecordSink> dataSinks )
    {
    // TODO: We should check that a record sink is taking data from only a record source
    synchronized( dataSinks )
      {
      this.dataSinks.clear( );
      this.dataSinks.addAll( dataSinks );
      for( RecordSink dataSink : dataSinks )
        {
        dataSink.setMetadata( this.metadata );
        }
      }
    }

  public void addPropertyChangeListener( PropertyChangeListener listener )
    {
    this.propertyChangeSupport.addPropertyChangeListener( listener );
    }

  public void removePropertyChangeListener( PropertyChangeListener listener )
    {
    this.propertyChangeSupport.removePropertyChangeListener( listener );
    }
  }
