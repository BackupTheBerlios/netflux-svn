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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.netflux.core.task.util.RecordComparator;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class MultiplexerTask extends AbstractTask
  {
  private static Log               log               = LogFactory.getLog( MultiplexerTask.class );
  private static ResourceBundle    messages          = ResourceBundle.getBundle( MultiplexerTask.class.getName( ) );

  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  public enum MultiplexerType
    {
    ROUND_ROBIN, SEQUENTIAL, ORDERED
    }

  protected MultiplexerType type;
  private List<String>      key = new ArrayList<String>( );

  /**
   * @param numberOfInputPorts
   */
  public MultiplexerTask( int numberOfInputPorts )
    {
    this( numberOfInputPorts, MultiplexerType.SEQUENTIAL, "MultiplexerTask|" + UUID.randomUUID( ).toString( ) );
    }

  /**
   * @param numberOfInputPorts
   * @param name
   */
  public MultiplexerTask( int numberOfInputPorts, String name )
    {
    this( numberOfInputPorts, MultiplexerType.SEQUENTIAL, name );
    }

  /**
   * @param numberOfInputPorts
   * @param type
   */
  public MultiplexerTask( int numberOfInputPorts, MultiplexerTask.MultiplexerType type )
    {
    this( numberOfInputPorts, type, "MultiplexerTask|" + UUID.randomUUID( ).toString( ) );
    }

  /**
   * @param numberOfInputPorts
   * @param type
   * @param name
   */
  public MultiplexerTask( int numberOfInputPorts, MultiplexerTask.MultiplexerType type, String name )
    {
    super( name, MultiplexerTask.computeInputPortNames( numberOfInputPorts ), MultiplexerTask.OUTPUT_PORT_NAMES );
    this.setType( type );
    }

  /**
   * @param numberOfInputPorts
   * @return
   */
  protected static Set<String> computeInputPortNames( int numberOfInputPorts )
    {
    Set<String> inputPortNames = new HashSet<String>( );
    for( int portIndex = 1; portIndex <= numberOfInputPorts; portIndex++ )
      {
      inputPortNames.add( "input" + portIndex );
      }
    return inputPortNames;
    }

  /**
   * @return Returns the type.
   */
  public MultiplexerType getType( )
    {
    return this.type;
    }

  /**
   * @param type The type to set.
   */
  public void setType( MultiplexerType type )
    {
    this.type = type;
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
    // TODO Check that all input ports have the same metadata??
    return newMetadata;
    }

  /**
   * @return
   */
  public RecordSink getInput1Port( )
    {
    return this.getInputPort( "input1" );
    }

  /**
   * @return
   */
  public RecordSink getInput2Port( )
    {
    return this.getInputPort( "input2" );
    }

  /**
   * @return
   */
  public RecordSink getInput3Port( )
    {
    return this.getInputPort( "input3" );
    }

  /**
   * @return
   */
  public RecordSink getInput4Port( )
    {
    return this.getInputPort( "input4" );
    }

  /**
   * @return
   */
  public RecordSink getInput5Port( )
    {
    return this.getInputPort( "input5" );
    }

  /**
   * @return
   */
  public RecordSink getInput6Port( )
    {
    return this.getInputPort( "input6" );
    }

  /**
   * @return
   */
  public RecordSink getInput7Port( )
    {
    return this.getInputPort( "input7" );
    }

  /**
   * @return
   */
  public RecordSink getInput8Port( )
    {
    return this.getInputPort( "input8" );
    }

  /**
   * @return
   */
  public RecordSink getInput9Port( )
    {
    return this.getInputPort( "input9" );
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
    switch( this.getType( ) )
      {
      case ROUND_ROBIN:
        return new MultiplexerTask.RoundRobinMultiplexerTaskWorker( );
      case SEQUENTIAL:
        return new MultiplexerTask.SequentialMultiplexerTaskWorker( );
      case ORDERED:
        return new MultiplexerTask.OrderedMultiplexerTaskWorker( );
      default:
        // TODO: Is this the right exception to throw?
        throw new IllegalStateException( );
      }
    }

  /**
   * @author jgonzalez
   */
  private class RoundRobinMultiplexerTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      if( MultiplexerTask.log.isInfoEnabled( ) )
        {
        String startedMessage = MultiplexerTask.messages.getString( "message.task.started" );
        MultiplexerTask.log.info( MessageFormat.format( startedMessage, MultiplexerTask.this.getName( ) ) );
        }

      OutputPort outputPort = MultiplexerTask.this.outputPorts.get( "output" );
      List<Integer> availablePorts = new LinkedList<Integer>( );
      for( int portIndex = 1; portIndex <= MultiplexerTask.this.getInputPorts( ).size( ); portIndex++ )
        {
        availablePorts.add( portIndex );
        }

      try
        {
        while( !availablePorts.isEmpty( ) )
          {
          Iterator<Integer> availablePortIndexIterator = availablePorts.iterator( );
          while( availablePortIndexIterator.hasNext( ) )
            {
            int portIndex = availablePortIndexIterator.next( );
            InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + portIndex );
            Record record = inputPort.getRecordQueue( ).take( );
            if( !record.equals( Record.END_OF_DATA ) )
              {
              if( MultiplexerTask.log.isTraceEnabled( ) )
                {
                MultiplexerTask.log.trace( "Outputting record from input port " + portIndex + ": " + record );
                }
              outputPort.consume( record );
              Thread.yield( );
              }
            else
              {
              MultiplexerTask.log.trace( "No more input available from port " + portIndex );
              availablePortIndexIterator.remove( );
              }
            }
          }
        }
      catch( InterruptedException exc )
        {
        MultiplexerTask.log.debug( "Exception while reading record", exc );
        }
      finally
        {
        outputPort.consume( Record.END_OF_DATA );

        if( MultiplexerTask.log.isInfoEnabled( ) )
          {
          String finishedMessage = MultiplexerTask.messages.getString( "message.task.finished" );
          MultiplexerTask.log.info( MessageFormat.format( finishedMessage, MultiplexerTask.this.getName( ) ) );
          }
        }
      }
    }

  /**
   * @author jgonzalez
   */
  private class SequentialMultiplexerTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      if( MultiplexerTask.log.isInfoEnabled( ) )
        {
        String startedMessage = MultiplexerTask.messages.getString( "message.task.started" );
        MultiplexerTask.log.info( MessageFormat.format( startedMessage, MultiplexerTask.this.getName( ) ) );
        }

      OutputPort outputPort = MultiplexerTask.this.outputPorts.get( "output" );
      try
        {
        for( int portIndex = 1; portIndex <= MultiplexerTask.this.getInputPorts( ).size( ); portIndex++ )
          {
          MultiplexerTask.log.trace( "Outputting records from port " + portIndex );
          InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + portIndex );
          Record record = inputPort.getRecordQueue( ).take( );
          while( !record.equals( Record.END_OF_DATA ) )
            {
            if( MultiplexerTask.log.isTraceEnabled( ) )
              {
              MultiplexerTask.log.trace( "Outputting record: " + record );
              }
            outputPort.consume( record );
            Thread.yield( );
            record = inputPort.getRecordQueue( ).take( );
            }
          }
        }
      catch( InterruptedException exc )
        {
        MultiplexerTask.log.debug( "Exception while reading record", exc );
        }
      finally
        {
        outputPort.consume( Record.END_OF_DATA );

        if( MultiplexerTask.log.isInfoEnabled( ) )
          {
          String finishedMessage = MultiplexerTask.messages.getString( "message.task.finished" );
          MultiplexerTask.log.info( MessageFormat.format( finishedMessage, MultiplexerTask.this.getName( ) ) );
          }
        }
      }
    }

  /**
   * @author jgonzalez
   */
  private class OrderedMultiplexerTaskWorker extends Thread
    {
    protected UUID insertOrderedRecord( List<Record> buffer, List<UUID> recordIDs, Record record, Comparator<Record> recordComparator )
      {
      UUID recordID = UUID.randomUUID( );
      // Insert the record in the right position.
      int insertionPoint = Collections.binarySearch( buffer, record, recordComparator );
      if( insertionPoint < 0 )
        {
        insertionPoint = -insertionPoint - 1;
        }
      buffer.add( insertionPoint, record );
      recordIDs.add( insertionPoint, recordID );

      return recordID;
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      if( MultiplexerTask.log.isInfoEnabled( ) )
        {
        String startedMessage = MultiplexerTask.messages.getString( "message.task.started" );
        MultiplexerTask.log.info( MessageFormat.format( startedMessage, MultiplexerTask.this.getName( ) ) );
        }

      OutputPort outputPort = MultiplexerTask.this.outputPorts.get( "output" );
      List<Record> orderedRecords = new ArrayList<Record>( );
      List<UUID> recordIDs = new ArrayList<UUID>( );
      Comparator<Record> recordComparator = new RecordComparator( MultiplexerTask.this.getKey( ) );
      Map<UUID, Integer> recordSource = new HashMap<UUID, Integer>( );

      try
        {
        // Initial population of orderedRecords and recordSource
        for( int portIndex = 1; portIndex <= MultiplexerTask.this.getInputPorts( ).size( ); portIndex++ )
          {
          InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + portIndex );
          Record record = inputPort.getRecordQueue( ).take( );
          if( !record.equals( Record.END_OF_DATA ) )
            {
            UUID recordID = this.insertOrderedRecord( orderedRecords, recordIDs, record, recordComparator );
            recordSource.put( recordID, portIndex );
            }
          else
            {
            MultiplexerTask.log.trace( "No more input available from port " + portIndex );
            }
          }

        while( !orderedRecords.isEmpty( ) )
          {
          // Get first record, and the port from where we read it, and remove them
          Record firstRecord = orderedRecords.remove( 0 );
          UUID firstRecordID = recordIDs.remove( 0 );
          int sourcePortIndex = recordSource.remove( firstRecordID );

          // Output of record
          if( MultiplexerTask.log.isTraceEnabled( ) )
            {
            MultiplexerTask.log.trace( "Outputting record from input port " + sourcePortIndex + ": " + firstRecord );
            }
          outputPort.consume( firstRecord );

          // We read a new record from the port where the record originated from
          InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + sourcePortIndex );
          Thread.yield( );
          Record record = inputPort.getRecordQueue( ).take( );
          if( !record.equals( Record.END_OF_DATA ) )
            {
            UUID recordID = this.insertOrderedRecord( orderedRecords, recordIDs, record, recordComparator );
            recordSource.put( recordID, sourcePortIndex );
            }
          else
            {
            MultiplexerTask.log.trace( "No more input available from port " + sourcePortIndex );
            }
          }
        }
      catch( InterruptedException exc )
        {
        MultiplexerTask.log.debug( "Exception while reading record", exc );
        }
      finally
        {
        outputPort.consume( Record.END_OF_DATA );

        if( MultiplexerTask.log.isInfoEnabled( ) )
          {
          String finishedMessage = MultiplexerTask.messages.getString( "message.task.finished" );
          MultiplexerTask.log.info( MessageFormat.format( finishedMessage, MultiplexerTask.this.getName( ) ) );
          }
        }
      }
    }
  }
