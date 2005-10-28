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
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;

/**
 * @author jgonzalez
 */
public class SimpleChannel implements Channel
  {
  protected PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport( this );
  protected RecordMetadata        metadata;
  protected List<RecordSink>      dataSinks             = new LinkedList<RecordSink>( );

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSource#getMetadata()
   */
  public RecordMetadata getMetadata( )
    {
    return this.metadata;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#setMetadata(org.netflux.core.RecordMetadata)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#consume(java.lang.Object)
   */
  public void consume( Record record )
    {
    for( RecordSink recordSink : this.dataSinks )
      {
      recordSink.consume( record );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSource#addDataSink(org.netflux.core.RecordSink)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSource#removeDataSink(org.netflux.core.RecordSink)
   */
  public void removeDataSink( RecordSink dataSink )
    {
    // TODO: We should do something with the record sink metadata
    synchronized( dataSinks )
      {
      this.dataSinks.remove( dataSink );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSource#setDataSinks(java.util.List)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#addPropertyChangeListener(java.beans.PropertyChangeListener)
   */
  public void addPropertyChangeListener( PropertyChangeListener listener )
    {
    this.propertyChangeSupport.addPropertyChangeListener( listener );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#removePropertyChangeListener(java.beans.PropertyChangeListener)
   */
  public void removePropertyChangeListener( PropertyChangeListener listener )
    {
    this.propertyChangeSupport.removePropertyChangeListener( listener );
    }
  }
