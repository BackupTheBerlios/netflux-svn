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

import java.util.Map;
import java.util.Set;

/**
 * <p>
 * An object that acts as a source of data. Data is delivered in the form of {@link Record}s going out through a set of named output
 * ports accessible through the provided methods. Those output ports are {@link RecordSource}s that may have some {@link RecordSink}s
 * registered to process the outgoing records. The records may be read from some physical storage, or come from some transformation.
 * </p>
 * <p>
 * In order to signal a <code>DataSource</code> to start delivering <code>Record</code>s, the
 * {@link org.netflux.core.DataSource#start()} method is provided. No record will be delivered before calling this method.
 * </p>
 * <p>
 * Although this interface provides methods to access any output port based on port names, it's convenient that implementing classes
 * add methods to retrieve those ports in a single call without parameters. That is, if we have a port called
 * <code>acceptedRecords</code>, provide a method with signature <code>public RecordSource getAcceptedRecordsPort()</code>. This
 * way output ports would be accessible using the JavaBeans standard, and this will ease a lot their use from dependency injection
 * frameworks.
 * </p>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public interface DataSource
  {
  /**
   * Returns the set of names of the output ports of this data sink. The returned set may be unmodifiable to prevent external
   * manipulation.
   * 
   * @return the names of the output ports of this data sink.
   */
  public Set<String> getOutputPortNames( );

  // TODO: Is this correct or should I use a bounded wildcard in the return type?
  /**
   * Returns a map relating port names to their corresponding output ports. The returned map may be unmodifiable to prevent external
   * manipulation.
   * 
   * @return the map relating output port names to their corresponding output ports.
   */
  public Map<String, RecordSource> getOutputPorts( );

  /**
   * Returns the output port with the supplied name. If no output port with the supplied name can be found, a <code>null</code> value
   * is returned.
   * 
   * @param portName the name of the port to retrieve.
   * @return the output port with the supplied name, <code>null</code> if no port with that name exists.
   */
  public RecordSource getOutputPort( String portName );

  /**
   * Invoked to make this <code>DataSource</code> start delivering records to all the <code>RecordSink</code>s registered in the
   * output ports.
   */
  public void start( );
  }
