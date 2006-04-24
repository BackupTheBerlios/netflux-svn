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
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.Channel;
import org.netflux.core.InvalidRecordMetadataException;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.util.RecordSinkSupport;
import org.netflux.core.util.RecordSourceSupport;

/**
 * A simple channel that directly forwards the records to process to the registered record sinks.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SimpleChannel implements Channel
  {
  private static Log              log                   = LogFactory.getLog( SimpleChannel.class );
  private static ResourceBundle   messages              = ResourceBundle.getBundle( SimpleChannel.class.getName( ) );

  protected PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport( this );
  protected RecordSourceSupport   recordSourceSupport;
  protected RecordSinkSupport     recordSinkSupport;

  /**
   * Creates a new <code>SimpleChannel</code>.
   */
  public SimpleChannel( )
    {
    this( "Channel|" + UUID.randomUUID( ) );
    }

  /**
   * Creates a new <code>SimpleChannel</code> with the specified <code>name</code>.
   * 
   * @param name the name of the channel
   */
  public SimpleChannel( String name )
    {
    this.recordSourceSupport = new RecordSourceSupport( name, this, this.propertyChangeSupport );
    this.recordSinkSupport = new RecordSinkSupport( name, this, this.propertyChangeSupport );
    }

  public String getName( )
    {
    return this.recordSinkSupport.getName( );
    }

  public void setName( String name )
    {
    this.recordSinkSupport.setName( name );
    this.recordSourceSupport.setName( name );
    }

  public RecordSource getRecordSource( )
    {
    return this.recordSinkSupport.getRecordSource( );
    }

  public void setRecordSource( RecordSource recordSource )
    {
    this.recordSinkSupport.setRecordSource( recordSource );
    }

  public void addRecordSink( RecordSink recordSink )
    {
    this.recordSourceSupport.addRecordSink( recordSink );
    }

  public void removeRecordSink( RecordSink recordSink )
    {
    this.recordSourceSupport.removeRecordSink( recordSink );
    }

  public void setRecordSinks( Set<RecordSink> recordSinks )
    {
    this.recordSourceSupport.setRecordSinks( recordSinks );
    }

  public void setRecordSink( RecordSink recordSink )
    {
    this.recordSourceSupport.setRecordSink( recordSink );
    }

  public RecordMetadata getMetadata( )
    {
    return this.recordSinkSupport.getMetadata( );
    }

  /**
   * Consumption implementation that just iterates over the list of registered <code>RecordSink</code>s calling its
   * {@link RecordSink#consume(Record)} method with the supplied <code>record</code>.
   */
  public void consume( Record record )
    {
    if( record.getMetadata( ).equals( this.getMetadata( ) ) || record.equals( Record.END_OF_DATA ) )
      {
      for( RecordSink recordSink : this.recordSourceSupport.getRecordSinks( ) )
        {
        if( SimpleChannel.log.isTraceEnabled( ) )
          SimpleChannel.log.trace( "Providing record to " + recordSink.getName( ) + " -> " + record );
        recordSink.consume( record );
        }
      }
    else
      {
      if( SimpleChannel.log.isInfoEnabled( ) )
        {
        SimpleChannel.log.info( SimpleChannel.messages.getString( "exception.invalid.metadata" ) );
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
