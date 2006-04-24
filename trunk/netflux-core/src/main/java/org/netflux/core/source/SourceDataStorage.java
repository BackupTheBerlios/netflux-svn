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
package org.netflux.core.source;

import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;

/**
 * A physical data storage where records can be taken from. This class is used to abstract away from any underlying storage medium
 * we're using. Typical implementations of this interface would include classes providing access to files or databases.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public interface SourceDataStorage
  {
  /**
   * Returns the metadata describing the data stored in this storage.
   * 
   * @return the metadata describing the data stored in this storage.
   */
  public RecordMetadata getMetadata( );

  /**
   * Returns the next record available in this data storage. If no more records are available it will return a
   * {@link Record#END_OF_DATA} record. In the event of any problem in the underlying storage, this method will throw a
   * <code>SourceDataStorageException</code>.
   * 
   * @return the next record available in the data storage, {@link Record#END_OF_DATA} if no more records are available.
   * @throws SourceDataStorageException if anything ugly happens in the underlying storage.
   */
  public Record nextRecord( ) throws SourceDataStorageException;

  /**
   * Invoked to give this data storage the opportunity to close any underlying resource. Any exception thrown by the underlying storage
   * while closing should be silently consumed by this method.
   */
  public void close( );
  }
