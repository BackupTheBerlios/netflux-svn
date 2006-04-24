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

import org.netflux.core.InvalidRecordMetadataException;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSource;

/**
 * <p>
 * Internal view of an output port for use in <code>DataSource</code>s and <code>Task</code>s.
 * </p>
 * <p>
 * <b>NOTE:</b> This class is an internal implementation detail. This has sense only for people writing <code>DataSource</code>s or
 * <code>Task</code>s and inheriting from <code>AbstractDataSource</code> or <code>AbstractTask</code>.
 * </p>
 * 
 * @author jgonzalez
 */
public interface OutputPort extends RecordSource
  {
  /**
   * Sets the metadata associated with this record sink. Once this metadata has been assigned, all the records provided to this record
   * sink must comply with the metadata provided.
   * 
   * @param metadata the metadata associated with this output port.
   */
  public void setMetadata( RecordMetadata metadata );

  /**
   * Invoked by the task or data source this output port is used by to indicate that a new record is available. This output port should
   * process the <code>record</code> without modifying it, dispatching it to every sink registered. This method doesn't accept
   * <code>null</code> records, if one is provided an exception will be thrown. The <code>record</code> won't be neither accepted
   * if its metadata doesn't fit in with this sink's metadata.
   * 
   * @param record the record to be processed by this output port.
   * @throws NullPointerException if the provided <code>record</code> is <code>null</code>.
   * @throws InvalidRecordMetadataException if the provided <code>record</code> has a metadata different than this output port's
   *           metadata.
   */
  public void consume( Record record );
  }
