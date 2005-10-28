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
package org.netflux.core.task.compose;

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

// TODO: Error handling
/**
 * @author jgonzalez
 */
public class DeduplicateTask extends AbstractTask
  {
  private static final Set<String> INPUT_PORT_NAMES  = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  /**
   * 
   */
  public DeduplicateTask( )
    {
    super( DeduplicateTask.INPUT_PORT_NAMES, DeduplicateTask.OUTPUT_PORT_NAMES );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.AbstractTask#updateMetadata(org.netflux.core.InputPort, org.netflux.core.RecordMetadata)
   */
  @Override
  public void updateMetadata( InputPort inputPort, RecordMetadata newMetadata )
    {
    for( Channel outputPort : this.outputPorts.values( ) )
      {
      outputPort.setMetadata( newMetadata );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#start()
   */
  public void start( )
    {
    // TODO: Thread handling
    new DeduplicateTaskWorker( ).start( );
    }

  public RecordSink getInputPort( )
    {
    return this.getInputPort( "input" );
    }

  /**
   * @return
   */
  public RecordSource getOutputPort( )
    {
    return this.getOutputPort( "output" );
    }


  /**
   * @author jgonzalez
   */
  private class DeduplicateTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      InputPort inputPort = DeduplicateTask.this.inputPorts.get( "input" );
      Channel outputPort = DeduplicateTask.this.outputPorts.get( "output" );
      Record lastRecord = null;
      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          if( lastRecord == null || !lastRecord.equals( record ) )
            {
            outputPort.consume( record );
            lastRecord = record;
            }

          record = inputPort.getRecordQueue( ).take( );
          }

        outputPort.consume( record );
        }
      catch( InterruptedException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      }
    }
  }
