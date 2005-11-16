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
package org.netflux.core.task.filter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.netflux.core.Channel;
import org.netflux.core.InputPort;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.task.AbstractTask;

/**
 * @author jgonzalez
 */
public class FilterTask extends AbstractTask
  {
  private static final Set<String> INPUT_PORT_NAMES  = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"accepted", "rejected"} ) );

  protected Filter                 filter;

  /**
   * 
   */
  public FilterTask( )
    {
    super( FilterTask.INPUT_PORT_NAMES, FilterTask.OUTPUT_PORT_NAMES );
    }

  /**
   * @return Returns the filter.
   */
  public Filter getFilter( )
    {
    return this.filter;
    }

  /**
   * @param filter The filter to set.
   */
  public void setFilter( Filter filter )
    {
    this.filter = filter;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.AbstractTask#computeMetadata(java.lang.String, org.netflux.core.InputPort,
   *      org.netflux.core.RecordMetadata)
   */
  @Override
  protected RecordMetadata computeMetadata( String outputPortName, InputPort changedInputPort, RecordMetadata newMetadata )
    {
    return newMetadata;
    }

  /**
   * @return
   */
  public RecordSink getInputPort( )
    {
    return this.getInputPort( "input" );
    }

  /**
   * @return
   */
  public RecordSource getAcceptedPort( )
    {
    return this.getOutputPort( "accepted" );
    }

  /**
   * @return
   */
  public RecordSource getRejectedPort( )
    {
    return this.getOutputPort( "rejected" );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.AbstractTask#getTaskWorker()
   */
  @Override
  protected Thread getTaskWorker( )
    {
    return new FilterTaskWorker( );
    }

  /**
   * @author jgonzalez
   */
  private class FilterTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      InputPort inputPort = FilterTask.this.inputPorts.get( "input" );
      Channel acceptedPort = FilterTask.this.outputPorts.get( "accepted" );
      Channel rejectedPort = FilterTask.this.outputPorts.get( "rejected" );
      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          if( FilterTask.this.filter.accepts( record ) )
            {
            acceptedPort.consume( record );
            }
          else
            {
            rejectedPort.consume( record );
            }

          Thread.yield( );
          record = inputPort.getRecordQueue( ).take( );
          }

        acceptedPort.consume( record );
        rejectedPort.consume( record );
        }
      catch( InterruptedException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      }
    }
  }
