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

import java.beans.PropertyChangeSupport;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;

/**
 * Support class providing common implementation of methods found in the {@link org.netflux.core.RecordSource} interface. Objects of
 * this class may be used in classes implementing the mentioned interface, providing suitable wrapper methods.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class RecordSourceSupport
  {
  private static Log            log         = LogFactory.getLog( RecordSourceSupport.class );

  private String                name;
  private RecordSource          recordSource;
  private HashSet<RecordSink>   recordSinks = new HashSet<RecordSink>( );
  private PropertyChangeSupport propertyChangeSupport;

  /**
   * Creates a <code>RecordSourceSupport</code> object with the provided parameters.
   * 
   * @param name the name of the record source this support object will be bounded to.
   * @param recordSink the record source this support object will be bounded to.
   * @param propertyChangeSupport the property change support object that will be used to fire property change events.
   */
  public RecordSourceSupport( String name, RecordSource recordSource, PropertyChangeSupport propertyChangeSupport )
    {
    this.name = name;
    this.recordSource = recordSource;
    this.propertyChangeSupport = propertyChangeSupport;
    }

  /**
   * Returns the name of the record source this support object is be bounded to.
   * 
   * @return the name of the record source this support object is be bounded to.
   */
  public String getName( )
    {
    return name;
    }

  /**
   * Sets the name of the record source this support object is be bounded to.
   * 
   * @param name the name of the record source this support object is be bounded to.
   */
  public void setName( String name )
    {
    String oldName = this.name;
    this.name = name;
    this.propertyChangeSupport.firePropertyChange( "name", oldName, this.name );
    }

  /**
   * Adds a record sink that will be able to read all the records coming out from the record source this support object is be bounded
   * to.
   * 
   * @param recordSink the record sink receiving records from the record source this support object is be bounded to.
   * @throws NullPointerException if the provided <code>recordSink</code> is <code>null</code>.
   */
  public void addRecordSink( RecordSink recordSink )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Adding new record sink" );
    synchronized( this.recordSinks )
      {
      if( !this.recordSinks.contains( recordSink ) )
        {
        Set<RecordSink> oldRecordSinks = (Set<RecordSink>) this.recordSinks.clone( );
        this.recordSinks.add( recordSink );
        recordSink.setRecordSource( this.recordSource );
        this.propertyChangeSupport.firePropertyChange( "recordSinks", oldRecordSinks, Collections.unmodifiableSet( this.recordSinks ) );
        }
      }
    }

  /**
   * Removes a record sink from the list of registered sinks of the record source this support object is be bounded to. The sink will
   * receive no more records coming out from the record source this support object is be bounded to.
   * 
   * @param recordSink the record sink to be removed.
   */
  public void removeRecordSink( RecordSink recordSink )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Removing record sink" );
    synchronized( this.recordSinks )
      {
      if( this.recordSinks.contains( recordSink ) )
        {
        Set<RecordSink> oldRecordSinks = (Set<RecordSink>) this.recordSinks.clone( );
        if( recordSink.getRecordSource( ) == this )
          {
          recordSink.setRecordSource( null );
          }
        this.recordSinks.remove( recordSink );
        this.propertyChangeSupport.firePropertyChange( "recordSinks", oldRecordSinks, Collections.unmodifiableSet( this.recordSinks ) );
        }
      }
    }

  /**
   * Returns the set of record sinks receiving records from the record source this support object is be bounded to.
   * 
   * @return the set of record sinks receiving records from the record source this support object is be bounded to.
   */
  public Set<RecordSink> getRecordSinks( )
    {
    return Collections.unmodifiableSet( this.recordSinks );
    }

  /**
   * Sets the set of record sinks that will be able to read all the records coming out from the record source this support object is be
   * bounded to. Any previously registered sinks will be unregistered, so the set provided will become the effective set of registered
   * sinks.
   * 
   * @param recordSinks the set of record sinks receiving records from the record source this support object is be bounded to.
   * @throws NullPointerException if the provided <code>recordSinks</code> parameter is <code>null</code>.
   */
  public void setRecordSinks( Set<RecordSink> recordSinks )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Setting record sinks" );
    synchronized( this.recordSinks )
      {
      Set<RecordSink> oldRecordSinks = (Set<RecordSink>) this.recordSinks.clone( );

      // Deregister previously registered record sinks
      Iterator<RecordSink> recordSinkIterator = this.recordSinks.iterator( );
      while( recordSinkIterator.hasNext( ) )
        {
        RecordSink recordSink = recordSinkIterator.next( );
        if( recordSink.getRecordSource( ) == this )
          {
          recordSink.setRecordSource( null );
          }
        recordSinkIterator.remove( );
        }

      // Register new record sinks
      this.recordSinks.addAll( recordSinks );
      for( RecordSink recordSink : recordSinks )
        {
        recordSink.setRecordSource( this.recordSource );
        }

      // Fire event
      this.propertyChangeSupport.firePropertyChange( "recordSinks", oldRecordSinks, Collections.unmodifiableSet( this.recordSinks ) );
      }
    }

  /**
   * Sets the record sink that will be able to read all the records coming out from the record source this support object is be bounded
   * to. Any previously registered sinks will be unregistered, so the record sink provided will become the only registered sink.
   * 
   * @param recordSink the record sink receiving records from the record source this support object is be bounded to.
   * @throws NullPointerException if the provided <code>recordSink</code> parameter is <code>null</code>.
   */
  public void setRecordSink( RecordSink recordSink )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Setting record sink" );
    this.setRecordSinks( Collections.singleton( recordSink ) );
    }
  }
