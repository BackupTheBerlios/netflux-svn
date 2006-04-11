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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.netflux.core.DataSource;
import org.netflux.core.RecordSource;
import org.netflux.core.flow.OutputPort;
import org.netflux.core.flow.SimpleOutputPort;

/**
 * Abstract basic implementation of <code>DataSource</code>. This class provides basic support for creation and management of output
 * ports, so any other implementation of <code>DataSource</code> could just inherit from this class and implement the missing
 * {@link org.netflux.core.DataSource#start()} method.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public abstract class AbstractDataSource implements DataSource
  {
  protected Map<String, OutputPort> outputPorts = new HashMap<String, OutputPort>( );
  private String                    name;

  /**
   * Creates a data source with a set of output ports named after <code>outputPortNames</code>.
   * 
   * @param outputPortNames the names of the output ports to be created.
   */
  protected AbstractDataSource( Set<String> outputPortNames )
    {
    this( "DataSource|" + UUID.randomUUID( ).toString( ), outputPortNames );
    }

  protected AbstractDataSource( String name, Set<String> outputPortNames )
    {
    this.name = name;
    for( String outputPortName : outputPortNames )
      {
      this.outputPorts.put( outputPortName, new SimpleOutputPort( ) );
      }
    }

  public String getName( )
    {
    return this.name;
    }

  /**
   * Sets the name of this data sink.
   * 
   * @param name the new name of the data sink
   */
  public void setName( String name )
    {
    this.name = name;
    for( Map.Entry<String, OutputPort> outputPortEntry : this.outputPorts.entrySet( ) )
      {
      outputPortEntry.getValue( ).setName( this.getName( ) + ":" + outputPortEntry.getKey( ) );
      }
    }

  public Set<String> getOutputPortNames( )
    {
    return this.outputPorts.keySet( );
    }

  public Map<String, RecordSource> getOutputPorts( )
    {
    // TODO: Is this correct or should I use a bounded wildcard in the return type?
    return new HashMap<String, RecordSource>( this.outputPorts );
    }

  public RecordSource getOutputPort( String portName )
    {
    return this.outputPorts.get( portName );
    }
  }
