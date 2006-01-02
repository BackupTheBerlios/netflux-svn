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
package org.netflux.core.source;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.netflux.core.Channel;
import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSource;

// TODO: Rethink if a error output port is suitable. Maybe this could be better solved using logging, as you shouldn't expect to
// process or recover error records.
/**
 * <p>
 * A simple data source that obtains data from a <code>SourceDataStorage</code>. This means this class may focus on port and thread
 * handling, as the main work of record retrieval is delegated to the <code>SourceDataStorage</code> object.
 * </p>
 * <p>
 * This data source has two ports:
 * </p>
 * <table border="1">
 * <tr>
 * <th>Port name</th>
 * <th>Description</th>
 * <th>Metadata</th>
 * </tr>
 * <tr>
 * <td>output</td>
 * <td>Records read from the underlying storage</td>
 * <td>As defined by the underlying storage</td>
 * </tr>
 * <tr>
 * <td>error</td>
 * <td>Records that produced some error while reading them from the underlying storage</td>
 * <td>
 * <ul>
 * <li><b>_recordNumber | integer</b> - number of the record causing the error</li>
 * <li><b>_message | varchar</b> - error message, if available</li>
 * <li><b>_source | varchar</b> - source if available (for example, line of text of source file)</li>
 * <li><b>record [n fields]</b> - copy of the partially populated record</li>
 * </ul>
 * </td>
 * </tr>
 * </table>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class SimpleDataSource extends AbstractDataSource
  {
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output", "error"} ) );

  private SourceDataStorage        sourceDataStorage;
  private RecordMetadata           errorMetadata;

  /**
   * Creates a new simple data source. The created data source will have two output ports: <code>output</code> and <code>error</code>.
   */
  public SimpleDataSource( )
    {
    super( SimpleDataSource.OUTPUT_PORT_NAMES );
    }

  /**
   * Returns the source data storage currently associated with this data source.
   * 
   * @return Returns the source data storage currently associated with this data source.
   */
  public SourceDataStorage getSourceDataStorage( )
    {
    return this.sourceDataStorage;
    }

  /**
   * Sets the source data storage where this data source will be reading records from.
   *  
   * @param sourceDataStorage the source data storage with records to read.
   */
  public void setSourceDataStorage( SourceDataStorage sourceDataStorage )
    {
    this.sourceDataStorage = sourceDataStorage;
    this.outputPorts.get( "output" ).setMetadata( this.sourceDataStorage.getMetadata( ) );

    List<FieldMetadata> errorFieldMetadata = new LinkedList<FieldMetadata>( this.sourceDataStorage.getMetadata( ).getFieldMetadata( ) );
    errorFieldMetadata.add( 0, new FieldMetadata( "_recordNumber", Types.INTEGER ) );
    errorFieldMetadata.add( 1, new FieldMetadata( "_message", Types.VARCHAR ) );
    errorFieldMetadata.add( 2, new FieldMetadata( "_source", Types.VARCHAR ) );
    this.errorMetadata = new RecordMetadata( errorFieldMetadata );
    this.outputPorts.get( "error" ).setMetadata( this.errorMetadata );
    }

  public void start( )
    {
    // TODO: Manage threads
    new SourceDataStorageWorker( ).start( );
    }

  /**
   * Returns the output port named <code>output</code>.
   * 
   * @return the output port named <code>output</code>.
   */
  public RecordSource getOutputPort( )
    {
    return this.getOutputPort( "output" );
    }

  /**
   * Returns the output port named <code>error</code>.
   * 
   * @return the output port named <code>error</code>.
   */
  public RecordSource getErrorPort( )
    {
    return this.getOutputPort( "error" );
    }

  private class SourceDataStorageWorker extends Thread
    {
    public SourceDataStorageWorker( )
      {
      // TODO: Create a UNIQUE name... maybe tasks should have a name property?
      super( "SourceDataStorageWorker" );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      Channel outputPort = SimpleDataSource.this.outputPorts.get( "output" );
      Channel errorPort = SimpleDataSource.this.outputPorts.get( "error" );
      SourceDataStorage storage = SimpleDataSource.this.getSourceDataStorage( );

      Record record = null;
      // TODO: This should probably be a long, or BigInteger
      int recordNumber = 0;
      do
        {
        try
          {
          recordNumber++;
          record = SimpleDataSource.this.getSourceDataStorage( ).nextRecord( );
          outputPort.consume( record );
          }
        catch( SourceDataStorageException exc )
          {
          // exc.printStackTrace( );
          Record errorRecord = new Record( SimpleDataSource.this.errorMetadata );
          errorRecord.setValue( "_recordNumber", recordNumber );
          errorRecord.setValue( "_message", " -- " + exc.getMessage( ) + " -- " );
          errorRecord.setValue( "_source", "  [[" + exc.getSource( ) + "]]  " );
          for( FieldMetadata fieldMetadata : exc.getPartialRecord( ).getMetadata( ).getFieldMetadata( ) )
            {
            errorRecord.setField( fieldMetadata.getName( ), exc.getPartialRecord( ).getField( fieldMetadata.getName( ) ) );
            }

          errorPort.consume( errorRecord );
          if( exc.isFatal( ) )
            {
            // TODO: handle exception
            record = Record.END_OF_DATA;
            outputPort.consume( record );
            }
          }
        }
      while( record != Record.END_OF_DATA );

      errorPort.consume( Record.END_OF_DATA );
      storage.close( );
      }
    }
  }
