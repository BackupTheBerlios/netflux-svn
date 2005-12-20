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

import java.util.concurrent.BlockingQueue;


// TODO: This should probably be moved to another location, as this is an internal implementation detail. This has sense only for
// people writing DataSinks or Tasks and inheriting from AbstractDataSink or AbstractTask.
/**
 * <p>
 * Internal view of an input port for use in <code>DataSink</code>s and <code>Task</code>s.
 * </p>
 * <p>
 * <b>NOTE:</b> This should probably be moved to another location, as this is an internal implementation detail. This has sense only
 * for people writing <code>DataSink</code>s or <code>Task</code>s and inheriting from <code>AbstractDataSink</code> or
 * <code>AbstractTask</code>.
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
  }
