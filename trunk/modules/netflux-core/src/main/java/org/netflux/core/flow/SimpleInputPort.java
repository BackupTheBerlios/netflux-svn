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
 * Simple implementation of an <code>InputPort</code> using a blocking queue to store records waiting to be processed.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SimpleInputPort implements InputPort
  {
  protected PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport( this );
  private RecordMetadata          metadata;
  private BlockingQueue<Record>   buffer;

  /**
   * Creates a new <code>SimpleInputPort</code>.
   */
  public SimpleInputPort( )
    {
    this.buffer = new LinkedBlockingQueue<Record>( );
    }

  /**
   * Creates a new <code>SimpleInputPort</code> with the specified initial <code>capacity</code> in its internal queue.
   * 
   * @param capacity the initial capacity of the internal queue.
   */
  public SimpleInputPort( int capacity )
    {
    this.buffer = new LinkedBlockingQueue<Record>( capacity );
    }

  /**
   * This method puts the specified <code>record</code> in the internal queue, waiting to be processed.
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

  public BlockingQueue<Record> getRecordQueue( )
    {
    return this.buffer;
    }

  public RecordMetadata getMetadata( )
    {
    return this.metadata;
    }

  public void setMetadata( RecordMetadata metadata )
    {
    RecordMetadata oldMetadata = this.metadata;
    this.metadata = metadata;
    this.propertyChangeSupport.firePropertyChange( "metadata", oldMetadata, this.metadata );
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
