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
 * An object that acts as a sink of data. Data is received in the form of {@link Record}s arriving through a set of named input ports
 * accessible through the provided methods. Those input ports are {@link RecordSink}s that may be connected to some record source to
 * get a flow of records to process. The records may be stored in some physical storage, logged or simply discarded.
 * </p>
 * <p>
 * Although this interface provides methods to access any input port based on port names, it's convenient that implementing classes add
 * methods to retrieve those ports in a single call without parameters. That is, if we have a port called <code>leftData</code>,
 * provide a method with signature <code>public RecordSink getLeftDataPort()</code>. This way input ports would be accessible using
 * the JavaBeans standard, and this will ease a lot their use from dependency injection frameworks.
 * </p>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public interface DataSink
  {
  /**
   * Returns the set of names of the input ports of this data sink. The returned set may be unmodifiable to prevent external
   * manipulation.
   * 
   * @return the names of the input ports of this data sink.
   */
  public Set<String> getInputPortNames( );

  // TODO: Is this correct or should we use a bounded wildcard in the return type?
  /**
   * Returns a map relating port names to their corresponding input ports. The returned map may be unmodifiable to prevent external
   * manipulation.
   * 
   * @return the map relating input port names to their corresponding input ports.
   */
  public Map<String, RecordSink> getInputPorts( );

  /**
   * Returns the input port with the supplied name. If no input port with the supplied name can be found, a <code>null</code> value
   * is returned.
   * 
   * @param portName the name of the port to retrieve.
   * @return the input port with the supplied name, <code>null</code> if no port with that name exists.
   */
  public RecordSink getInputPort( String portName );
  }
