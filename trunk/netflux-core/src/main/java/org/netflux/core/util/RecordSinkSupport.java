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
package org.netflux.core.util;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;

/**
 * Support class providing common implementation of methods found in the {@link org.netflux.core.RecordSink} interface. Objects of this
 * class may be used in classes implementing the mentioned interface, providing suitable wrapper methods.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class RecordSinkSupport
  {
  private static Log               log                  = LogFactory.getLog( RecordSinkSupport.class );

  private String                   name;
  private RecordSink               recordSink;
  private RecordSource             recordSource;
  private RecordMetadata           metadata;
  private PropertyChangeSupport    propertyChangeSupport;
  protected PropertyChangeListener recordSourceListener = new PropertyChangeListener( )
                                                          {
                                                            public void propertyChange( PropertyChangeEvent event )
                                                              {
                                                              RecordSinkSupport.this
                                                                  .setMetadata( (RecordMetadata) event.getNewValue( ) );
                                                              }
                                                          };

  /**
   * Creates a <code>RecordSinkSupport</code> object with the provided parameters.
   * 
   * @param name the name of the record sink this support object will be bounded to.
   * @param recordSink the record sink this support object will be bounded to.
   * @param propertyChangeSupport the property change support object that will be used to fire property change events.
   */
  public RecordSinkSupport( String name, RecordSink recordSink, PropertyChangeSupport propertyChangeSupport )
    {
    this.name = name;
    this.recordSink = recordSink;
    this.propertyChangeSupport = propertyChangeSupport;
    }

  /**
   * Returns the name of the record sink this support object is be bounded to.
   * 
   * @return the name of the record sink this support object is be bounded to.
   */
  public String getName( )
    {
    return name;
    }

  /**
   * Sets the name of the record sink this support object is be bounded to.
   * 
   * @param name the name of the record sink this support object is be bounded to.
   */
  public void setName( String name )
    {
    String oldName = this.name;
    this.name = name;
    this.propertyChangeSupport.firePropertyChange( "name", oldName, this.name );
    }

  /**
   * Gets the record source that is currently providing records to the record sink this support object is be bounded to. If no record
   * source has been registered, this method returns <code>null</code>.
   * 
   * @return the record source currently providing records to the record sink this support object is be bounded to, <code>null</code>
   *         if none available.
   */
  public RecordSource getRecordSource( )
    {
    return this.recordSource;
    }

  /**
   * Sets the record source that will provide records to the record sink this support object is be bounded to.
   * 
   * @param recordSource the record source that will provide records to the record sink this support object is be bounded to,
   *          <code>null</code> if the record source is being detached from the record sink this support object is be bounded to.
   */
  public void setRecordSource( RecordSource recordSource )
    {
    if( this.recordSource != recordSource )
      {
      RecordSinkSupport.log.debug( this.getName( ) + " - Setting new record source" );
      if( this.recordSource != null )
        {
        RecordSinkSupport.log.trace( this.getName( ) + " - Removing property change listener of prior record source" );
        this.recordSource.removePropertyChangeListener( "metadata", this.recordSourceListener );
        this.recordSource.removeRecordSink( this.recordSink );
        }

      RecordSource oldRecordSource = this.recordSource;
      this.recordSource = recordSource;
      if( this.recordSource != null )
        {
        this.setMetadata( this.recordSource.getMetadata( ) );
        RecordSinkSupport.log.trace( this.getName( ) + " - Setting property change listener for new record source" );
        this.recordSource.addRecordSink( this.recordSink );
        this.recordSource.addPropertyChangeListener( "metadata", this.recordSourceListener );
        }
      else
        {
        RecordSinkSupport.log.debug( this.getName( ) + " - New record source is null - no record source for this record sink" );
        this.setMetadata( null );
        }

      this.propertyChangeSupport.firePropertyChange( "recordSource", oldRecordSource, this.recordSource );
      }
    }

  /**
   * Returns the metadata associated with the currently registered record source, <code>null</code> if no record source is currently
   * registered.
   * 
   * @return the metadata associated with the currently registered record source, <code>null</code> if no record source is currently
   *         registered.
   */
  public RecordMetadata getMetadata( )
    {
    return this.metadata;
    }

  /**
   * Sets the metadata of accepted records by the record sink this support object is be bounded to.
   * 
   * @param metadata the metadata of accepted records by the record sink this support object is be bounded to.
   */
  protected void setMetadata( RecordMetadata metadata )
    {
    RecordSinkSupport.log.debug( this.getName( ) + " - Setting new metadata" );
    RecordMetadata oldMetadata = this.metadata;
    this.metadata = metadata;
    this.propertyChangeSupport.firePropertyChange( "metadata", oldMetadata, this.metadata );
    if( RecordSinkSupport.log.isTraceEnabled( ) )
      RecordSinkSupport.log.trace( this.getName( ) + " - Metadata changed from " + oldMetadata + " to " + this.metadata );
    }
  }
