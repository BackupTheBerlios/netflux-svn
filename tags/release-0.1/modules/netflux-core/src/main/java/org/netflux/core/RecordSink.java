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

/**
 * <p>
 * An object that acts as a sink of {@link Record}s. The records may be stored in some physical storage, transformed into some output
 * records or simply discarded. All the records coming into this sink must share the same metadata, that must be set before providing
 * any record using the provided method. There is only one exception to this rule: we may use the special {@link Record#END_OF_DATA}
 * record to notify this sink that there is no more available data from the source providing records.
 * </p>
 * <p>
 * A <code>RecordSink</code> fires a {@link java.beans.PropertyChangeEvent} whenever the input metadata changes, so any other object
 * using this sink is aware of any change in the input metadata.
 * </p>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public interface RecordSink
  {
  /**
   * Add a <code>PropertyChangeListener</code> to the listener list. The listener is registered for all properties. The same listener
   * object may be added more than once, and will be called as many times as it is added. If <code>listener</code> is
   * <code>null</code>, no exception is thrown and no action is taken.
   * 
   * @param listener the <code>PropertyChangeListener</code> to be added.
   */
  public void addPropertyChangeListener( PropertyChangeListener listener );

  /**
   * Remove a <code>PropertyChangeListener</code> from the listener list. If <code>listener</code> was added more than once to the
   * same event source, it will be notified one less time after being removed. If <code>listener</code> is <code>null</code>, or
   * was never added, no exception is thrown and no action is taken.
   * 
   * @param listener the <code>PropertyChangeListener</code> to be removed.
   */
  public void removePropertyChangeListener( PropertyChangeListener listener );

  /**
   * Sets the metadata associated with this record sink. Once this metadata has been assigned, all the records provided to this record
   * sink must comply with the metadata provided.
   * 
   * @param metadata the metadata associated with this record sink.
   */
  public void setMetadata( RecordMetadata metadata );

  /**
   * Invoked by the record source this sink is registered in to indicate that a new record is available. This data sink should process
   * the <code>record</code> without modifying it, as the same <code>record</code> will have been handed to every sink registered
   * in the same record source the provided <code>record</code> originated from. This method doesn't accept <code>null</code>
   * records, if one is provided an exception will be thrown. The <code>record</code> won't be neither accepted if its metadata
   * doesn't fit in with this sink's metadata.
   * 
   * @param record the record to be processed by this record sink.
   * @throws NullPointerException if the provided <code>record</code> is <code>null</code>.
   * @throws InvalidRecordMetadataException if the provided <code>record</code> has a metadata different than this sink's metadata.
   */
  public void consume( Record record );
  }
