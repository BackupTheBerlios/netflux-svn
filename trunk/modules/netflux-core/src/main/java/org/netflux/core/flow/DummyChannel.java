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

import java.beans.PropertyChangeListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.Record;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;

/**
 * A <code>RecordSink</code> that simply discards all the records that receives.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class DummyChannel implements RecordSink
  {
  private static Log   log = LogFactory.getLog( DummyChannel.class );

  private String       name;
  private RecordSource recordSource;

  public String getName( )
    {
    return this.name;
    }

  public void setName( String name )
    {
    this.name = name;
    }

  public void setRecordSource( RecordSource recordSource )
    {
    DummyChannel.log.debug( "Setting record source of dummy channel" );
    this.recordSource = recordSource;
    }

  public RecordSource getRecordSource( )
    {
    return this.recordSource;
    }

  public void consume( Record record )
    {
    if( DummyChannel.log.isTraceEnabled( ) )
      {
      DummyChannel.log.trace( "Discarding record in dummy channel: " + record );
      }
    }

  public void addPropertyChangeListener( PropertyChangeListener listener )
    {}

  public void removePropertyChangeListener( PropertyChangeListener listener )
    {}
  }
