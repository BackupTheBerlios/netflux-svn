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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.netflux.core.DataSink;
import org.netflux.core.InputPort;
import org.netflux.core.RecordSink;
import org.netflux.core.flow.SimpleInputPort;

/**
 * Abstract basic implementation of <code>DataSink</code>. This class provides basic support for creation and management of input
 * ports, so any other implementation of <code>DataSink</code> could just inherit from this class and implement the missing behavior.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public abstract class AbstractDataSink implements DataSink
  {
  protected Map<String, InputPort> inputPorts = new HashMap<String, InputPort>( );

  /**
   * Creates a data sink with a set of input ports named after <code>inputPortNames</code>.
   * 
   * @param inputPortNames the names of the input ports to be created.
   */
  protected AbstractDataSink( Set<String> inputPortNames )
    {
    for( String inputPortName : inputPortNames )
      {
      this.inputPorts.put( inputPortName, new SimpleInputPort( ) );
      }
    }

  public Set<String> getInputPortNames( )
    {
    return this.inputPorts.keySet( );
    }

  public Map<String, RecordSink> getInputPorts( )
    {
    // TODO: Is this correct or should I use a bounded wildcard in the return type?
    return new HashMap<String, RecordSink>( this.inputPorts );
    }

  public RecordSink getInputPort( String portName )
    {
    return this.inputPorts.get( portName );
    }
  }
