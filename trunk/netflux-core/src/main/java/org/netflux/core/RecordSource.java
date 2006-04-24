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
package org.netflux.core;

import java.beans.PropertyChangeListener;
import java.util.Set;

/**
 * <p>
 * An object that acts as a source of {@link Record}s. The records may be read from some physical storage, transformed from some input
 * records or generated in some other way. All the records coming out from a <code>RecordSource</code> must share the same metadata,
 * that we can retrieve using the provided method. There is only one exception to this rule: we may use the special
 * {@link Record#END_OF_DATA} record to notify all the registered sinks that there is no more available data from this source.
 * </p>
 * <p>
 * The only way to get data from a <code>RecordSource</code> is to provide a {@link RecordSink} to be registered in the list of sinks
 * of this source. Every time a new record is available, this source will provide such record to all the registered sinks using the
 * appropiate method of the <code>RecordSink</code> interface.
 * </p>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public interface RecordSource
  {
  /**
   * Returns the name of this record source.
   * 
   * @return the name of this record source.
   */
  public String getName( );

  /**
   * Sets the name of this record source.
   * 
   * @param name the name of this record source.
   */
  public void setName( String name );

  /**
   * Returns the metadata associated with this record source. All the records coming out of this source will have this same metadata.
   * 
   * @return the metadata associated with this record source.
   */
  public RecordMetadata getMetadata( );

  /**
   * Adds a record sink that will be able to read all the records coming out from this source.
   * 
   * @param recordSink the record sink receiving records from this source.
   * @throws NullPointerException if the provided <code>recordSink</code> is <code>null</code>.
   */
  public void addRecordSink( RecordSink recordSink );

  /**
   * Removes a record sink from the list of registered sinks of this record source. The sink will receive no more records coming out
   * from this source.
   * 
   * @param recordSink the record sink to be removed.
   */
  public void removeRecordSink( RecordSink recordSink );

  /**
   * Sets the set of record sinks that will be able to read all the records coming out from this source. Any previously registered
   * sinks will be unregistered, so the set provided will become the effective set of registered sinks.
   * 
   * @param recordSinks the set of record sinks receiving records from this source.
   * @throws NullPointerException if the provided <code>recordSinks</code> parameter is <code>null</code>.
   */
  public void setRecordSinks( Set<RecordSink> recordSinks );

  /**
   * Sets the record sink that will be able to read all the records coming out from this source. Any previously registered sinks will
   * be unregistered, so the record sink provided will become the only registered sink.
   * 
   * @param recordSink the record sink receiving records from this source.
   * @throws NullPointerException if the provided <code>recordSink</code> parameter is <code>null</code>.
   */
  public void setRecordSink( RecordSink recordSink );

  /**
   * Add a <code>PropertyChangeListener</code> to the listener list. The listener is registered for all properties. The same listener
   * object may be added more than once, and will be called as many times as it is added. If <code>listener</code> is
   * <code>null</code>, no exception is thrown and no action is taken.
   * 
   * @param listener the <code>PropertyChangeListener</code> to be added.
   */
  public void addPropertyChangeListener( PropertyChangeListener listener );

  /**
   * Add a <code>PropertyChangeListener</code> for a specific property. The listener will be invoked only when a call on
   * <code>firePropertyChange</code> names that specific property. The same listener object may be added more than once. For each
   * property, the listener will be invoked the number of times it was added for that property. If <code>propertyName</code> or
   * <code>listener</code> is null, no exception is thrown and no action is taken.
   * 
   * @param propertyName the name of the property to listen on.
   * @param listener the <code>PropertyChangeListener</code> to be added.
   */
  public void addPropertyChangeListener( String propertyName, PropertyChangeListener listener );

  /**
   * Remove a <code>PropertyChangeListener</code> from the listener list. If <code>listener</code> was added more than once to the
   * same event source, it will be notified one less time after being removed. If <code>listener</code> is <code>null</code>, or
   * was never added, no exception is thrown and no action is taken.
   * 
   * @param listener the <code>PropertyChangeListener</code> to be removed.
   */
  public void removePropertyChangeListener( PropertyChangeListener listener );

  /**
   * Remove a <code>PropertyChangeListener</code> for a specific property. If <code>listener</code> was added more than once to the
   * same event source for the specified property, it will be notified one less time after being removed. If <code>propertyName</code>
   * is null, no exception is thrown and no action is taken. If <code>listener</code> is null, or was never added for the specified
   * property, no exception is thrown and no action is taken.
   * 
   * @param listener the <code>PropertyChangeListener</code> to be removed.
   */
  public void removePropertyChangeListener( String propertyName, PropertyChangeListener listener );
  }
