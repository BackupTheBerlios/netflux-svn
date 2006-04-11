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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.netflux.core.FieldMetadata;
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
public class SplitTask extends AbstractTask
  {
  private static final Set<String> INPUT_PORT_NAMES       = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES      = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  protected List<List<String>>     fieldNamesToSplit      = new LinkedList<List<String>>( );
  protected Set<String>            fieldNamesToSplitAsSet = new HashSet<String>( );
  protected List<String>           splittedFieldNames     = new LinkedList<String>( );

  /**
   * 
   */
  public SplitTask( )
    {
    super( SplitTask.INPUT_PORT_NAMES, SplitTask.OUTPUT_PORT_NAMES );
    }

  /**
   * @return Returns the fieldNamesToSplit.
   */
  public List<List<String>> getFieldNamesToSplit( )
    {
    return this.fieldNamesToSplit;
    }

  /**
   * @param fieldNamesToSplit The fieldNamesToSplit to set.
   */
  public void setFieldNamesToSplit( List<List<String>> fieldNamesToSplit )
    {
    this.fieldNamesToSplit = fieldNamesToSplit;
    this.fieldNamesToSplitAsSet = new HashSet<String>( );
    for( List<String> nameGroup : this.fieldNamesToSplit )
      {
      this.fieldNamesToSplitAsSet.addAll( nameGroup );
      }
    }

  /**
   * @return Returns the splittedFieldNames.
   */
  public List<String> getSplittedFieldNames( )
    {
    return this.splittedFieldNames;
    }

  /**
   * @param splittedFieldNames The splittedFieldNames to set.
   */
  public void setSplittedFieldNames( List<String> splittedFieldNames )
    {
    this.splittedFieldNames = splittedFieldNames;
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
    int insertionPoint = Integer.MAX_VALUE;
    RecordMetadata inputMetadata = this.inputPorts.get( "input" ).getMetadata( );

    if( inputMetadata != null && inputMetadata.getFieldCount( ) > 0 )
      {
      List<FieldMetadata> fieldMetadata = new LinkedList<FieldMetadata>( inputMetadata.getFieldMetadata( ) );
      ListIterator<FieldMetadata> fieldMetadataIterator = fieldMetadata.listIterator( );
      while( fieldMetadataIterator.hasNext( ) )
        {
        FieldMetadata currentFieldMetadata = fieldMetadataIterator.next( );
        if( this.fieldNamesToSplitAsSet.contains( currentFieldMetadata.getName( ) ) )
          {
          insertionPoint = Math.min( insertionPoint, fieldMetadataIterator.previousIndex( ) );
          fieldMetadataIterator.remove( );
          }
        }

      List<FieldMetadata> splittedMetadata = new LinkedList<FieldMetadata>( );
      Iterator<String> splittedFieldNameIterator = this.splittedFieldNames.iterator( );
      for( String fieldName : this.fieldNamesToSplit.get( 0 ) )
        {
        // FIXME: Do this properly: getFieldMetadata should throw an Exception
        if( inputMetadata.getFieldIndex( fieldName ) != -1 )
          {
          FieldMetadata currentSplittedMetadata = inputMetadata.getFieldMetadata( fieldName ).clone( );
          currentSplittedMetadata.setName( splittedFieldNameIterator.next( ) );
          splittedMetadata.add( currentSplittedMetadata );
          }
        else
          {
          splittedMetadata.clear( );
          insertionPoint = fieldMetadata.size( );
          break;
          }
        }

      fieldMetadata.addAll( insertionPoint, splittedMetadata );

      return new RecordMetadata( fieldMetadata );
      }
    else
      {
      List<FieldMetadata> emptyMetadata = Collections.emptyList( );
      return new RecordMetadata( emptyMetadata );
      }
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
    return new SplitTaskWorker( );
    }

  /**
   * @author jgonzalez
   */
  private class SplitTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      InputPort inputPort = SplitTask.this.inputPorts.get( "input" );
      OutputPort outputPort = SplitTask.this.outputPorts.get( "output" );
      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          for( List<String> nameGroup : SplitTask.this.fieldNamesToSplit )
            {
            Record splittedRecord = new Record( outputPort.getMetadata( ) );

            boolean nonNullFound = false;
            Iterator<String> splittedFieldNameIterator = SplitTask.this.splittedFieldNames.iterator( );
            for( String fieldName : nameGroup )
              {
              nonNullFound |= record.getField( fieldName ).getValue( ) != null;
              splittedRecord.setField( splittedFieldNameIterator.next( ), record.getField( fieldName ) );
              }

            // TODO: This should be configurable, shouldn't it?
            if( nonNullFound )
              {
              for( FieldMetadata fieldMetadata : record.getMetadata( ).getFieldMetadata( ) )
                {
                String fieldName = fieldMetadata.getName( );
                if( !SplitTask.this.fieldNamesToSplitAsSet.contains( fieldName ) )
                  {
                  splittedRecord.setField( fieldName, record.getField( fieldName ) );
                  }
                }

              outputPort.consume( splittedRecord );
              }
            }

          Thread.yield( );
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
