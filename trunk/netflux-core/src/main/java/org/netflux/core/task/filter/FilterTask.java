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

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
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

/**
 * <p>
 * A task that filters input records using a given filter. The task will read every record from its input port, evaluate a filter on
 * the record, and if the record passes the filter, it will write the record to the accepted port; otherwise it will write it to the
 * rejected port.
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
 * <td>Records to be filtered, using the given filter.</td>
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
 * <td>accepted</td>
 * <td>Same as input record</td>
 * <td>Records accepted by the given filter.</td>
 * </tr>
 * <tr>
 * <td>rejected</td>
 * <td>Same as input record</td>
 * <td>Records rejected by the given filter.</td>
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
 * <td>filter</td>
 * <td>{@link org.netflux.core.task.filter.Filter}</td>
 * <td><code>null</code></td>
 * <td>Filter to be applied on input records. If no filter is supplied all records will be rejected.</td>
 * </tr>
 * </tbody> </table>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class FilterTask extends AbstractTask
  {
  private static Log               log               = LogFactory.getLog( FilterTask.class );
  private static ResourceBundle    messages          = ResourceBundle.getBundle( FilterTask.class.getName( ) );

  private static final Set<String> INPUT_PORT_NAMES  = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"accepted", "rejected"} ) );

  protected Filter                 filter;

  /**
   * Creates a new filter task.
   */
  public FilterTask( )
    {
    this( "FilterTask|" + UUID.randomUUID( ).toString( ) );
    }

  /**
   * Creates a new filter task with the provided name.
   * 
   * @param name the name of the new filter task.
   */
  public FilterTask( String name )
    {
    super( name, FilterTask.INPUT_PORT_NAMES, FilterTask.OUTPUT_PORT_NAMES );
    }

  /**
   * Returns the filter associated with this filter task. The filter will be responsible for accepting or rejecting input records, that
   * will be written to the accepted or rejected port respectively.
   * 
   * @return the filter used to accept or reject records, <code>null</code> if no filter has been assigned.
   */
  public Filter getFilter( )
    {
    return this.filter;
    }

  /**
   * Sets the filter associated with this filter task. The filter will be responsible for accepting or rejecting input records, that
   * will be written to the accepted or rejected port respectively. If the filter is <code>null</code>, all records will be
   * rejected.
   * 
   * @param filter the filter used to accept or reject records.
   */
  public void setFilter( Filter filter )
    {
    this.filter = filter;
    }

  @Override
  protected RecordMetadata computeMetadata( String outputPortName, InputPort changedInputPort, RecordMetadata newMetadata )
    {
    return newMetadata;
    }

  /**
   * Returns the input port of this task.
   * 
   * @return the input port of this task.
   */
  public RecordSink getInputPort( )
    {
    return this.getInputPort( "input" );
    }

  /**
   * Returns the output port where accepted records will be written.
   * 
   * @return the accepted records output port.
   */
  public RecordSource getAcceptedPort( )
    {
    return this.getOutputPort( "accepted" );
    }

  /**
   * Returns the output port where rejected records will be written.
   * 
   * @return the rejected records output port.
   */
  public RecordSource getRejectedPort( )
    {
    return this.getOutputPort( "rejected" );
    }

  @Override
  protected Thread getTaskWorker( )
    {
    return new FilterTaskWorker( );
    }

  private class FilterTaskWorker extends Thread
    {
    @Override
    public void run( )
      {
      if( FilterTask.log.isInfoEnabled( ) )
        {
        String startedMessage = FilterTask.messages.getString( "message.task.started" );
        FilterTask.log.info( MessageFormat.format( startedMessage, FilterTask.this.getName( ) ) );
        }

      InputPort inputPort = FilterTask.this.inputPorts.get( "input" );
      OutputPort acceptedPort = FilterTask.this.outputPorts.get( "accepted" );
      OutputPort rejectedPort = FilterTask.this.outputPorts.get( "rejected" );
      Filter filter = FilterTask.this.getFilter( );

      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          if( filter != null && filter.accepts( record ) )
            {
            if( FilterTask.log.isTraceEnabled( ) )
              {
              FilterTask.log.trace( "Record accepted: " + record );
              }
            acceptedPort.consume( record );
            }
          else
            {
            if( FilterTask.log.isTraceEnabled( ) )
              {
              FilterTask.log.trace( "Record rejected: " + record );
              }
            rejectedPort.consume( record );
            }

          Thread.yield( );
          record = inputPort.getRecordQueue( ).take( );
          }
        }
      catch( InterruptedException exc )
        {
        FilterTask.log.debug( "Exception while reading record", exc );
        }
      finally
        {
        acceptedPort.consume( Record.END_OF_DATA );
        rejectedPort.consume( Record.END_OF_DATA );

        if( FilterTask.log.isInfoEnabled( ) )
          {
          String finishedMessage = FilterTask.messages.getString( "message.task.finished" );
          FilterTask.log.info( MessageFormat.format( finishedMessage, FilterTask.this.getName( ) ) );
          }
        }
      }
    }
  }
