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
package org.netflux.core.sink;

import org.netflux.core.Record;

/**
 * A physical data storage where records can be stored in. This class is used to abstract away from any underlying storage medium we're
 * using. Typical implementations of this interface would include classes providing access to files or databases.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public interface TargetDataStorage
  {
  /**
   * Stores a record in this data storage. In the event of any problem in the underlying storage, this method will throw a
   * <code>TargetDataStorageException</code>.
   * 
   * @param record the record to be stored in the data storage.
   * @throws TargetDataStorageException if anything ugly happens in the underlying storage.
   */
  public void storeRecord( Record record ) throws TargetDataStorageException;

  /**
   * Invoked to give this data storage the opportunity to close any underlying resource. Any exception thrown by the underlying storage
   * while closing should be silently consumed by this method.
   */
  public void close( );
  }
