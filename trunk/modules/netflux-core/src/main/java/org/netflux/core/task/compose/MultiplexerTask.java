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
import org.netflux.core.RecordSource;
import org.netflux.core.task.AbstractTask;

/**
 * @author jgonzalez
 */
public class MultiplexerTask extends AbstractTask
  {
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  // TODO: Implement all types, right now just sequential implemented
  public enum MultiplexerType
    {
    ROUND_ROBIN, SEQUENTIAL, ORDERED
    }

  /**
   * 
   */
  public MultiplexerTask( int numberOfInputPorts )
    {
    super( MultiplexerTask.computeInputPortNames( numberOfInputPorts ), MultiplexerTask.OUTPUT_PORT_NAMES );
    }

  protected static Set<String> computeInputPortNames( int numberOfInputPorts )
    {
    Set<String> inputPortNames = new HashSet<String>( );
    for( int portIndex = 1; portIndex <= numberOfInputPorts; portIndex++ )
      {
      inputPortNames.add( "input" + portIndex );
      }
    return inputPortNames;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.AbstractTask#updateMetadata(org.netflux.core.InputPort, org.netflux.core.RecordMetadata)
   */
  @Override
  public void updateMetadata( InputPort inputPort, RecordMetadata newMetadata )
    {
    this.outputPorts.get( "output" ).setMetadata( newMetadata );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#start()
   */
  public void start( )
    {
    // TODO Handle threads
    new MultiplexerTask.MultiplexerTaskWorker( ).start( );
    }

  /**
   * @return
   */
  public RecordSource getOutputPort( )
    {
    return this.getOutputPort( "output" );
    }

  private class MultiplexerTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      Channel outputPort = MultiplexerTask.this.outputPorts.get( "output" );
      for( int portIndex = 1; portIndex <= MultiplexerTask.this.getInputPorts( ).size( ); portIndex++ )
        {
        InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + portIndex );
        try
          {
          Record record = inputPort.getRecordQueue( ).take( );
          while( !record.equals( Record.END_OF_DATA ) )
            {
            outputPort.consume( record );
            record = inputPort.getRecordQueue( ).take( );
            }

          }
        catch( InterruptedException exc )
          {
          // TODO: handle exception
          exc.printStackTrace( );
          }
        }
      outputPort.consume( Record.END_OF_DATA );
      }
    }
  }