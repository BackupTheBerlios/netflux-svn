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

import java.util.List;

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
   * Returns the metadata associated with this record source. All the records coming out of this source will have this same metadata.
   * 
   * @return the metadata associated with this record source.
   */
  public RecordMetadata getMetadata( );

  /**
   * Adds a record sink that will be able to read all the records coming out from this source.
   * 
   * @param dataSink the data sink receiving records from this source.
   * @throws NullPointerException if the provided <code>dataSink</code> is <code>null</code>.
   */
  public void addDataSink( RecordSink dataSink );

  /**
   * Removes a record sink from the list of registered sinks of this record source. The sink will receive no more records coming out
   * from this source.
   * 
   * @param dataSink the data sink to be removed.
   */
  public void removeDataSink( RecordSink dataSink );

  // TODO: Maybe this should be a Set, there has no sense to have a duplicated data sink receiving the same record twice, and order
  // is not that important here
  /**
   * Sets the list of data sinks that will be able to read all the records coming out from this source. Any previously registered sink
   * will be unregistered, so the list provided will become the effective list of registered sinks.
   * 
   * @param dataSinks
   * @throws NullPointerException if the provided <code>dataSinks</code> parameter is <code>null</code>.
   */
  public void setDataSinks( List<RecordSink> dataSinks );
  }
