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
import java.util.ListIterator;
import java.util.Set;
import java.util.UUID;

import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.flow.InputPort;
import org.netflux.core.flow.OutputPort;
import org.netflux.core.task.AbstractTask;
import org.netflux.core.task.util.RecordComparator;

/**
 * <p>
 * A task that sorts input records. The records will be sorted according to the specified ordering, comparing the specified key. In
 * order to compare fields of a record, the {@link org.netflux.core.Record#compareTo(Record)} method is used, so the records extracted
 * using the supplied key must comply with the restrictions specified in that method.
 * </p>
 * <p>
 * This task implements also partial sorting, this is, if the input records are already sorted using a given key, and you want to order
 * each group of records with the same key using an additional group of fields, you may do so using a proper combination of
 * <code>partiallyOrderedKey</code> and <code>key</code> parameters.
 * </p>
 * <p>
 * This task uses an internal buffer to store records until all records have been received and sorted, but you may impose a maximum
 * size on this buffer. Once this size has been reached, the task writes the record currently held in the first (or last) position,
 * reads a new record and inserts it in the buffer in the correct position. This behaviour is repeated until all records have been
 * written. In the case of partial sorting, every time a group of records with the same key has been sorted, the buffer is emptied.
 * </p>
 * <p>
 * <b><i>Input ports:</b></i>
 * </p>
 * <table border="1" cellpadding="3" cellspacing="0"> <thead>
 * <tr>
 * <th>Name</th>
 * <th>Description</th>
 * </tr>
 * </thead> <tbody align="left" valign="top">
 * <tr>
 * <td>input</td>
 * <td>Records to be sorted.</td>
 * </tr>
 * </tbody> </table>
 * <p>
 * <b><i>Output ports:</b></i>
 * </p>
 * <table border="1" cellpadding="3" cellspacing="0"> <thead>
 * <tr>
 * <th>Name</th>
 * <th>Metadata</th>
 * <th>Description</th>
 * </tr>
 * </thead> <tbody align="left" valign="top">
 * <tr>
 * <td>output</td>
 * <td>Same as input record</td>
 * <td>Records sorted in the specified order, using the specified key.</td>
 * </tr>
 * </tbody> </table>
 * <p>
 * <b><i>Parameters:</b></i>
 * </p>
 * <table border="1" cellpadding="3" cellspacing="0"> <thead>
 * <tr>
 * <th>Name</th>
 * <th>Type</th>
 * <th>Default</th>
 * <th>Description</th>
 * </tr>
 * </thead> <tbody align="left" valign="top">
 * <tr>
 * <td>partiallyOrderedKey</td>
 * <td>{@link java.util.List}&lt;{@link java.lang.String}&gt;</td>
 * <td>Empty list</td>
 * <td>If this parameter is specified, the task will perform a partial sort. That is, it will assume that input records are sorted
 * according to the key specified in this parameter, and will only sort records in groups of records that share this key. Input records
 * must be sorted according to the specified ordering and key supplied in this parameter, otherwise this task may not function
 * properly. If this parameter is not provided, the task will sort all the records.</td>
 * </tr>
 * <tr>
 * <td>key</td>
 * <td>{@link java.util.List}&lt;{@link java.lang.String}&gt;</td>
 * <td>Empty list</td>
 * <td>List of fields used to sort the records.</td>
 * </tr>
 * <tr>
 * <td>ordering</td>
 * <td>{@link SortTask.Ordering}</td>
 * <td>{@link SortTask.Ordering#ASCENDING}</td>
 * <td>Order of records sorted, either ascending or descending.</td>
 * </tr>
 * <tr>
 * <td>bufferSize</td>
 * <td>int</td>
 * <td>{@link java.lang.Integer#MAX_VALUE}</td>
 * <td>Size of the buffer used to store records while sorting them. If no value is specified, the buffer will have no size limit.</td>
 * </tr>
 * </tbody> </table>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SortTask extends AbstractTask
  {
  public enum Ordering
    {
    ASCENDING, DESCENDING
    }

  private static final Set<String> INPUT_PORT_NAMES    = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES   = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  private List<String>             partiallyOrderedKey = new ArrayList<String>( );
  private List<String>             key                 = new ArrayList<String>( );
  private SortTask.Ordering        ordering            = SortTask.Ordering.ASCENDING;
  private int                      bufferSize          = Integer.MAX_VALUE;

  /**
   * Creates a new sort task.
   */
  public SortTask( )
    {
    this( "SortTask|" + UUID.randomUUID( ).toString( ) );
    }

  /**
   * Creates a new sort task with the provided name.
   * 
   * @param name the name of the new sort task.
   */
  /**
   * 
   */
  public SortTask( String name )
    {
    super( name, SortTask.INPUT_PORT_NAMES, SortTask.OUTPUT_PORT_NAMES );
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
   * @return
   */
  public SortTask.Ordering getOrdering( )
    {
    return ordering;
    }

  /**
   * @param ordering
   */
  public void setOrdering( SortTask.Ordering ordering )
    {
    this.ordering = ordering;
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

  @Override
  protected RecordMetadata computeMetadata( String outputPortName, InputPort changedInputPort, RecordMetadata newMetadata )
    {
    return newMetadata;
    }

  public RecordSink getInputPort( )
    {
    return this.getInputPort( "input" );
    }

  public RecordSource getOutputPort( )
    {
    return this.getOutputPort( "output" );
    }

  @Override
  protected Thread getTaskWorker( )
    {
    return new SortTaskWorker( );
    }

  private class SortTaskWorker extends Thread
    {
    @Override
    public void run( )
      {
      InputPort inputPort = SortTask.this.inputPorts.get( "input" );
      OutputPort outputPort = SortTask.this.outputPorts.get( "output" );
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
            Record firstRecord;
            switch( SortTask.this.getOrdering( ) )
              {
              case DESCENDING:
                firstRecord = buffer.remove( buffer.size( ) - 1 );
                break;

              case ASCENDING:
              default:
                // Ascending order as default case
                firstRecord = buffer.remove( 0 );
                break;
              }
            outputPort.consume( firstRecord );
            }

          Thread.yield( );
          record = inputPort.getRecordQueue( ).take( );
          }
        }
      catch( InterruptedException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      finally
        {
        this.outputAndClearBuffer( outputPort, buffer );
        outputPort.consume( Record.END_OF_DATA );
        }
      }

    protected void outputAndClearBuffer( OutputPort outputPort, List<Record> buffer )
      {
      switch( SortTask.this.getOrdering( ) )
        {
        case DESCENDING:
          ListIterator<Record> descendingBufferIterator = buffer.listIterator( buffer.size( ) );
          while( descendingBufferIterator.hasPrevious( ) )
            {
            outputPort.consume( descendingBufferIterator.previous( ) );
            }
          break;

        case ASCENDING:
        default:
          // Ascending order as default case
          Iterator<Record> ascendingBufferIterator = buffer.iterator( );
          while( ascendingBufferIterator.hasNext( ) )
            {
            outputPort.consume( ascendingBufferIterator.next( ) );
            }
          break;
        }

      buffer.clear( );
      }
    }
  }
