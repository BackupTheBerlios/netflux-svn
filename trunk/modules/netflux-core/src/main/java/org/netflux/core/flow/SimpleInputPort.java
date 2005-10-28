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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.netflux.core.InputPort;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

/**
 * @author jgonzalez
 */
public class SimpleInputPort implements InputPort
  {
  protected PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport( this );
  protected RecordMetadata        metadata;
  protected BlockingQueue<Record> buffer;

  /**
   * 
   */
  public SimpleInputPort( )
    {
    this.buffer = new LinkedBlockingQueue<Record>( );
    }

  /**
   * 
   */
  public SimpleInputPort( int capacity )
    {
    this.buffer = new LinkedBlockingQueue<Record>( capacity );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.flow.SimpleChannel#consume(java.lang.Object)
   */
  public void consume( Record record )
    {
    if( record.getMetadata( ).equals( this.metadata ) || record.equals( Record.END_OF_DATA ) )
      {
      try
        {
        this.buffer.put( record );
        }
      catch( InterruptedException exc )
        {
        // TODO Auto-generated catch block
        exc.printStackTrace( );
        }
      }
    else
      {
      throw new IllegalArgumentException( );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.InputPort#getRecordQueue()
   */
  public BlockingQueue<Record> getRecordQueue( )
    {
    return this.buffer;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.InputPort#getMetadata()
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
    this.propertyChangeSupport.firePropertyChange( "metadata", oldMetadata, this.metadata );
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
