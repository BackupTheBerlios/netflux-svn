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
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.InvalidRecordMetadataException;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSource;
import org.netflux.core.util.RecordSinkSupport;

/**
 * Simple implementation of an <code>InputPort</code> using a blocking queue to store records waiting to be processed.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SimpleInputPort implements InputPort
  {
  private static Log              log                   = LogFactory.getLog( SimpleInputPort.class );
  private static ResourceBundle   messages              = ResourceBundle.getBundle( SimpleInputPort.class.getName( ) );

  protected PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport( this );
  protected RecordSinkSupport     recordSinkSupport;
  private BlockingQueue<Record>   buffer;

  /**
   * Creates a new <code>SimpleInputPort</code>.
   */
  public SimpleInputPort( )
    {
    this( "InputPort|" + UUID.randomUUID( ).toString( ) );
    }

  /**
   * Creates a new <code>SimpleInputPort</code> with the specified <code>name</code>.
   * 
   * @param name the name of the port
   */
  public SimpleInputPort( String name )
    {
    this.recordSinkSupport = new RecordSinkSupport( name, this, this.propertyChangeSupport );
    this.buffer = new LinkedBlockingQueue<Record>( );
    }

  /**
   * Creates a new <code>SimpleInputPort</code> with the specified initial <code>capacity</code> in its internal queue.
   * 
   * @param capacity the initial capacity of the internal queue.
   */
  public SimpleInputPort( int capacity )
    {
    this( UUID.randomUUID( ).toString( ), capacity );
    }

  /**
   * Creates a new <code>SimpleInputPort</code> with the specified <code>name</code> and initial <code>capacity</code> in its
   * internal queue.
   * 
   * @param name the name of the port
   * @param capacity the initial capacity of the internal queue.
   */
  public SimpleInputPort( String name, int capacity )
    {
    this.recordSinkSupport = new RecordSinkSupport( name, this, this.propertyChangeSupport );
    this.buffer = new LinkedBlockingQueue<Record>( capacity );
    }

  public String getName( )
    {
    return this.recordSinkSupport.getName( );
    }

  public void setName( String name )
    {
    this.recordSinkSupport.setName( name );
    }

  public RecordSource getRecordSource( )
    {
    return this.recordSinkSupport.getRecordSource( );
    }

  public void setRecordSource( RecordSource recordSource )
    {
    this.recordSinkSupport.setRecordSource( recordSource );
    }

  public BlockingQueue<Record> getRecordQueue( )
    {
    return this.buffer;
    }

  public RecordMetadata getMetadata( )
    {
    return this.recordSinkSupport.getMetadata( );
    }

  /**
   * This method puts the specified <code>record</code> in the internal queue, waiting to be processed.
   */
  public void consume( Record record )
    {
    if( record.getMetadata( ).equals( this.getMetadata( ) ) || record.equals( Record.END_OF_DATA ) )
      {
      try
        {
        if( SimpleInputPort.log.isTraceEnabled( ) )
          SimpleInputPort.log.trace( "Storing record in internal queue: " + record );
        this.buffer.put( record );
        }
      catch( InterruptedException exc )
        {
        SimpleInputPort.log.debug( "Interrupted while putting record in queue", exc );
        }
      }
    else
      {
      if( SimpleInputPort.log.isInfoEnabled( ) )
        {
        SimpleInputPort.log.info( SimpleInputPort.messages.getString( "exception.invalid.metadata" ) );
        }
      throw new InvalidRecordMetadataException( record, this.getMetadata( ) );
      }
    }

  public void addPropertyChangeListener( PropertyChangeListener listener )
    {
    this.propertyChangeSupport.addPropertyChangeListener( listener );
    }

  public void addPropertyChangeListener( String propertyName, PropertyChangeListener listener )
    {
    this.propertyChangeSupport.addPropertyChangeListener( propertyName, listener );
    }

  public void removePropertyChangeListener( PropertyChangeListener listener )
    {
    this.propertyChangeSupport.removePropertyChangeListener( listener );
    }

  public void removePropertyChangeListener( String propertyName, PropertyChangeListener listener )
    {
    this.propertyChangeSupport.removePropertyChangeListener( propertyName, listener );
    }
  }
