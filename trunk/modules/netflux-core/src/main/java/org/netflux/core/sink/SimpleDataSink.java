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
package org.netflux.core.sink;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.netflux.core.InputPort;
import org.netflux.core.Record;
import org.netflux.core.RecordSink;

/**
 * <p>
 * A simple data sink that stores data in a <code>TargetDataStorage</code>. This means this class may focus on port and thread
 * handling, as the main work of record storage is delegated to the <code>TargetDataStorage</code> object.
 * </p>
 * <p>
 * This data sink has one port:
 * </p>
 * <table border="1">
 * <tr>
 * <th>Port name</th>
 * <th>Description</th>
 * <th>Metadata</th>
 * </tr>
 * <tr>
 * <td>input</td>
 * <td>Records to be stored in the underlying storage</td>
 * <td>As handled by the underlying storage</td>
 * </tr>
 * </table>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SimpleDataSink extends AbstractDataSink
  {
  private static final Set<String> INPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );

  private TargetDataStorage        targetDataStorage;
  private TargetDataStorageWorker  worker;

  /**
   * Creates a new simple data sink. The created data sink will have one input ports named <code>input</code>.
   */
  public SimpleDataSink( )
    {
    super( SimpleDataSink.INPUT_PORT_NAMES );
    this.worker = new TargetDataStorageWorker( );
    }

  /**
   * Returns the target data storage currently associated with this data sink.
   * 
   * @return Returns the target data storage currently associated with this data sink.
   */
  public TargetDataStorage getTargetDataStorage( )
    {
    return this.targetDataStorage;
    }

  /**
   * Sets the target data storage where this data sink will be storing records in.
   * 
   * @param targetDataStorage the target data storage where records will be stored.
   */
  public void setTargetDataStorage( TargetDataStorage targetDataStorage )
    {
    this.targetDataStorage = targetDataStorage;
    this.worker.start( );
    }

  /**
   * Returns the input port.
   * 
   * @return the input port.
   */
  public RecordSink getInputPort( )
    {
    return this.getInputPort( "input" );
    }

  private class TargetDataStorageWorker extends Thread
    {
    public TargetDataStorageWorker( )
      {
      // TODO Auto-generated constructor stub
      super( "TargetDataStorageWorker" );
      }

    @Override
    public void run( )
      {
      InputPort inputPort = SimpleDataSink.this.inputPorts.get( "input" );
      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          SimpleDataSink.this.targetDataStorage.storeRecord( record );
          record = inputPort.getRecordQueue( ).take( );
          }
        }
      catch( InterruptedException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      catch( Exception exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      finally
        {
        SimpleDataSink.this.targetDataStorage.close( );
        }
      }
    }
  }
