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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.flow.InputPort;
import org.netflux.core.flow.OutputPort;
import org.netflux.core.task.AbstractTask;

// TODO: Error handling
/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class DeduplicateTask extends AbstractTask
  {
  private static Log               log               = LogFactory.getLog( DeduplicateTask.class );
  private static ResourceBundle    messages          = ResourceBundle.getBundle( DeduplicateTask.class.getName( ) );

  private static final Set<String> INPUT_PORT_NAMES  = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  private List<String>             key               = new ArrayList<String>( );

  /**
   * Creates a new deduplicate task.
   */
  public DeduplicateTask( )
    {
    this( "DeduplicateTask|" + UUID.randomUUID( ).toString( ) );
    }

  /**
   * Creates a new deduplicate task with the provided name.
   * 
   * @param name the name of the new deduplicate task.
   */
  public DeduplicateTask( String name )
    {
    super( name, DeduplicateTask.INPUT_PORT_NAMES, DeduplicateTask.OUTPUT_PORT_NAMES );
    }

  /**
   * @return Returns the key.
   */
  public List<String> getKey( )
    {
    return this.key;
    }

  /**
   * @param key The key to set.
   */
  public void setKey( List<String> key )
    {
    this.key = key;
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
  public RecordSource getOutputPort( )
    {
    return this.getOutputPort( "output" );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.AbstractTask#getTaskWorker()
   */
  @Override
  protected Thread getTaskWorker( )
    {
    return new DeduplicateTaskWorker( );
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
      if( DeduplicateTask.log.isInfoEnabled( ) )
        {
        String startedMessage = DeduplicateTask.messages.getString( "message.task.started" );
        DeduplicateTask.log.info( MessageFormat.format( startedMessage, DeduplicateTask.this.getName( ) ) );
        }

      InputPort inputPort = DeduplicateTask.this.inputPorts.get( "input" );
      OutputPort outputPort = DeduplicateTask.this.outputPorts.get( "output" );
      List<String> key = DeduplicateTask.this.getKey( );

      Record lastRecord = null;
      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          boolean differentRecordFound = false;
          if( lastRecord == null )
            {
            differentRecordFound = true;
            }
          else if( key == null || key.isEmpty( ) )
            {
            differentRecordFound = !lastRecord.equals( record );
            }
          else
            {
            differentRecordFound = !lastRecord.extract( key ).equals( record.extract( key ) );
            }

          if( differentRecordFound )
            {
            if( DeduplicateTask.log.isTraceEnabled( ) )
              {
              DeduplicateTask.log.trace( "Outputting record: " + record );
              }
            outputPort.consume( record );
            lastRecord = record;
            }
          else
            {
            if( DeduplicateTask.log.isTraceEnabled( ) )
              {
              DeduplicateTask.log.trace( "Discarding record: " + record );
              }
            }

          Thread.yield( );
          record = inputPort.getRecordQueue( ).take( );
          }
        }
      catch( InterruptedException exc )
        {
        DeduplicateTask.log.debug( "Exception while reading record", exc );
        }
      finally
        {
        outputPort.consume( Record.END_OF_DATA );

        if( DeduplicateTask.log.isInfoEnabled( ) )
          {
          String finishedMessage = DeduplicateTask.messages.getString( "message.task.finished" );
          DeduplicateTask.log.info( MessageFormat.format( finishedMessage, DeduplicateTask.this.getName( ) ) );
          }
        }
      }
    }
  }
