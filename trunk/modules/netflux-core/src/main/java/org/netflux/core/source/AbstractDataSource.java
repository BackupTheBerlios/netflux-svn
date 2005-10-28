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

import org.netflux.core.Channel;
import org.netflux.core.DataSource;
import org.netflux.core.RecordSource;
import org.netflux.core.flow.SimpleChannel;

/**
 * @author jgonzalez
 */
public abstract class AbstractDataSource implements DataSource
  {
  protected Map<String, Channel> outputPorts = new HashMap<String, Channel>( );

  /**
   * @param outputPortNames
   */
  protected AbstractDataSource( Set<String> outputPortNames )
    {
    for( String outputPortName : outputPortNames )
      {
      this.outputPorts.put( outputPortName, new SimpleChannel( ) );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#getOutputPortNames()
   */
  public Set<String> getOutputPortNames( )
    {
    return this.outputPorts.keySet( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#getOutputPorts()
   */
  public Map<String, RecordSource> getOutputPorts( )
    {
    // TODO: Is this correct or should I use a bounded wildcard in the return type?
    return new HashMap<String, RecordSource>( this.outputPorts );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#getOutputPort(java.lang.String)
   */
  public RecordSource getOutputPort( String portName )
    {
    return this.outputPorts.get( portName );
    }
  }
