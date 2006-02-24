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
package org.netflux.core.task.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.netflux.core.Channel;
import org.netflux.core.InputPort;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.task.AbstractTask;
import org.netflux.core.task.util.RecordComparator;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SortTask extends AbstractTask
  {
  private static final Set<String> INPUT_PORT_NAMES    = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES   = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  private List<String>             partiallyOrderedKey = new ArrayList<String>( );
  private List<String>             key                 = new ArrayList<String>( );
  private int                      bufferSize          = Integer.MAX_VALUE;

  /**
   * 
   */
  public SortTask( )
    {
    super( SortTask.INPUT_PORT_NAMES, SortTask.OUTPUT_PORT_NAMES );
    }

  /**
   * @return Returns the partiallyOrderedKey.
   */
  public List<String> getPartiallyOrderedKey( )
    {
    return this.partiallyOrderedKey;
    }

  /**
   * @param partiallyOrderedKey The partiallyOrderedKey to set.
   */
  public void setPartiallyOrderedKey( List<String> partiallyOrderedKey )
    {
    this.partiallyOrderedKey = partiallyOrderedKey;
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

  /**
   * @return Returns the bufferSize.
   */
  public int getBufferSize( )
    {
    return this.bufferSize;
    }

  /**
   * @param bufferSize The bufferSize to set.
   */
  public void setBufferSize( int bufferSize )
    {
    this.bufferSize = bufferSize;
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
    return new SortTaskWorker( );
    }

  /**
   * @author jgonzalez
   */
  private class SortTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      InputPort inputPort = SortTask.this.inputPorts.get( "input" );
      Channel outputPort = SortTask.this.outputPorts.get( "output" );
      List<Record> buffer = new ArrayList<Record>( );
      Comparator<Record> recordComparator = new RecordComparator( SortTask.this.getKey( ) );
      Record lastKey = null;

      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          // Are we doing a partial sort? If so, act accordingly
          if( !SortTask.this.getPartiallyOrderedKey( ).isEmpty( ) )
            {
            if( lastKey != null )
              {
              if( !record.extract( SortTask.this.getPartiallyOrderedKey( ) ).equals( lastKey ) )
                {
                this.outputAndClearBuffer( outputPort, buffer );
                lastKey = record.extract( SortTask.this.getPartiallyOrderedKey( ) );
                }
              }
            else
              {
              lastKey = record.extract( SortTask.this.getPartiallyOrderedKey( ) );
              }
            }

          // Insert the record in the right position.
          int position = Collections.binarySearch( buffer, record, recordComparator );
          if( position >= 0 )
            {
            buffer.add( position, record );
            }
          else
            {
            int insertionPoint = -position - 1;
            buffer.add( insertionPoint, record );
            }

          // If we have reached the maximum buffer size we take out records... in this case the order may be incorrect
          // TODO: Show a warning???
          if( buffer.size( ) >= SortTask.this.getBufferSize( ) )
            {
            Record firstRecord = buffer.remove( 0 );
            outputPort.consume( firstRecord );
            }

          Thread.yield( );
          record = inputPort.getRecordQueue( ).take( );
          }

        this.outputAndClearBuffer( outputPort, buffer );
        outputPort.consume( Record.END_OF_DATA );
        }
      catch( InterruptedException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      }

    protected void outputAndClearBuffer( Channel outputPort, List<Record> buffer )
      {
      Iterator<Record> bufferIterator = buffer.iterator( );
      while( bufferIterator.hasNext( ) )
        {
        outputPort.consume( bufferIterator.next( ) );
        }
      buffer.clear( );
      }
    }
  }
