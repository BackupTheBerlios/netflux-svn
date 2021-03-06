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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.netflux.core.Channel;
import org.netflux.core.InputPort;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.task.AbstractTask;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class MultiplexerTask extends AbstractTask
  {
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  // TODO: Implement all types, right now just sequential implemented
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
    this( numberOfInputPorts, MultiplexerType.SEQUENTIAL );
    }

  /**
   * @param numberOfInputPorts
   * @param type
   */
  public MultiplexerTask( int numberOfInputPorts, MultiplexerTask.MultiplexerType type )
    {
    super( MultiplexerTask.computeInputPortNames( numberOfInputPorts ), MultiplexerTask.OUTPUT_PORT_NAMES );
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
      Channel outputPort = MultiplexerTask.this.outputPorts.get( "output" );
      List<Integer> availablePorts = new LinkedList<Integer>( );
      for( int portIndex = 1; portIndex <= MultiplexerTask.this.getInputPorts( ).size( ); portIndex++ )
        {
        availablePorts.add( portIndex );
        }

      while( !availablePorts.isEmpty( ) )
        {
        Iterator<Integer> availablePortIndexIterator = availablePorts.iterator( );
        while( availablePortIndexIterator.hasNext( ) )
          {
          int portIndex = availablePortIndexIterator.next( );
          InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + portIndex );
          try
            {
            Record record = inputPort.getRecordQueue( ).take( );
            if( !record.equals( Record.END_OF_DATA ) )
              {
              outputPort.consume( record );
              Thread.yield( );
              }
            else
              {
              availablePortIndexIterator.remove( );
              }
            }
          catch( InterruptedException exc )
            {
            // TODO: handle exception
            exc.printStackTrace( );
            }
          }
        }
      outputPort.consume( Record.END_OF_DATA );
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
            Thread.yield( );
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

  /**
   * @author jgonzalez
   */
  private class OrderedMultiplexerTaskWorker extends Thread
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
      SortedSet<Record> orderedRecords = new TreeSet<Record>( new MultiplexerTask.RecordComparator( MultiplexerTask.this.getKey( ) ) );
      Map<Record, Integer> recordSource = new HashMap<Record, Integer>( );
      // Initial population of orderedRecords and recordSource
      for( int portIndex = 1; portIndex <= MultiplexerTask.this.getInputPorts( ).size( ); portIndex++ )
        {
        InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + portIndex );
        try
          {
          Record record = inputPort.getRecordQueue( ).take( );
          if( !record.equals( Record.END_OF_DATA ) )
            {
            orderedRecords.add( record );
            recordSource.put( record, portIndex );
            }
          }
        catch( InterruptedException exc )
          {
          // TODO Auto-generated catch block
          exc.printStackTrace( );
          }
        }

      while( !orderedRecords.isEmpty( ) )
        {
        // Get first record, and the port from where we read it
        Record firstRecord = orderedRecords.first( );
        int sourcePortIndex = recordSource.get( firstRecord );

        // Remove records
        orderedRecords.remove( firstRecord );
        recordSource.remove( firstRecord );

        // Output of record
        outputPort.consume( firstRecord );

        // We read a new record from the port where the record originated from
        InputPort inputPort = MultiplexerTask.this.inputPorts.get( "input" + sourcePortIndex );
        try
          {
          Thread.yield( );
          Record record = inputPort.getRecordQueue( ).take( );
          if( !record.equals( Record.END_OF_DATA ) )
            {
            orderedRecords.add( record );
            recordSource.put( record, sourcePortIndex );
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

  /**
   * @author jgonzalez
   */
  private static class RecordComparator implements Comparator<Record>
    {
    private List<String> key = new ArrayList<String>( );

    /**
     * 
     */
    public RecordComparator( List<String> key )
      {
      this.key = key;
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Comparator#compare(T, T)
     */
    public int compare( Record firstRecord, Record secondRecord )
      {
      // TODO Auto-generated method stub
      if( this.key == null || this.key.isEmpty( ) )
        {
        return firstRecord.compareTo( secondRecord );
        }
      else
        {
        return firstRecord.extract( this.key ).compareTo( secondRecord.extract( this.key ) );
        }
      }
    }
  }
