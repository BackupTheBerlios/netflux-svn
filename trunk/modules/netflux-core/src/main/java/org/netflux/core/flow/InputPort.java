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
import java.util.concurrent.BlockingQueue;

import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;

/**
 * <p>
 * Internal view of an input port for use in <code>DataSink</code>s and <code>Task</code>s.
 * </p>
 * <p>
 * <b>NOTE:</b> This class is an internal implementation detail. This has sense only for people writing <code>DataSink</code>s or
 * <code>Task</code>s and inheriting from <code>AbstractDataSink</code> or <code>AbstractTask</code>.
 * </p>
 * 
 * @author jgonzalez
 */
public interface InputPort extends RecordSink
  {
  /**
   * Returns the metadata associated with this input port.
   * 
   * @return the metadata associated with this input port.
   */
  public RecordMetadata getMetadata( );

  /**
   * Returns the queue where all the records to be processed are stored.
   * 
   * @return the queue with records to be processed.
   */
  public BlockingQueue<Record> getRecordQueue( );

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
