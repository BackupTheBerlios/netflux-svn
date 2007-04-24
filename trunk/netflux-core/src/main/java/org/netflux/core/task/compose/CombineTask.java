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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class CombineTask extends AbstractTask
  {
  private static Log               log                      = LogFactory.getLog( CombineTask.class );
  private static ResourceBundle    messages                 = ResourceBundle.getBundle( CombineTask.class.getName( ) );

  private static final Set<String> INPUT_PORT_NAMES         = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES        = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  protected List<String>           fieldNamesToCombine      = new LinkedList<String>( );
  protected Set<String>            fieldNamesToCombineAsSet = new HashSet<String>( );
  protected List<List<String>>     combinedFieldNames       = new LinkedList<List<String>>( );
  protected List<String>           groupingKeyFieldNames    = new LinkedList<String>( );

  /**
   * Creates a new combine task.
   */
  public CombineTask( )
    {
    this( "CombineTask|" + UUID.randomUUID( ).toString( ) );
    }

  /**
   * Creates a new combine task with the provided name.
   * 
   * @param name the name of the new combine task.
   */
  public CombineTask( String name )
    {
    super( name, CombineTask.INPUT_PORT_NAMES, CombineTask.OUTPUT_PORT_NAMES );
    }

  /**
   * @return Returns the fieldNamesToCombine.
   */
  public List<String> getFieldNamesToCombine( )
    {
    return this.fieldNamesToCombine;
    }

  /**
   * @param fieldNamesToCombine The fieldNamesToCombine to set.
   */
  public void setFieldNamesToCombine( List<String> fieldNamesToCombine )
    {
    this.fieldNamesToCombine = fieldNamesToCombine;
    this.fieldNamesToCombineAsSet = new HashSet<String>( fieldNamesToCombine );
    }

  /**
   * @return Returns the combinedFieldNames.
   */
  public List<List<String>> getCombinedFieldNames( )
    {
    return this.combinedFieldNames;
    }

  /**
   * @param combinedFieldNames The combinedFieldNames to set.
   */
  public void setCombinedFieldNames( List<List<String>> combinedFieldNames )
    {
    this.combinedFieldNames = combinedFieldNames;
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
        if( this.fieldNamesToCombineAsSet.contains( currentFieldMetadata.getName( ) ) )
          {
          insertionPoint = Math.min( insertionPoint, fieldMetadataIterator.previousIndex( ) );
          fieldMetadataIterator.remove( );
          }
        }

      this.groupingKeyFieldNames = new ArrayList<String>( new RecordMetadata( fieldMetadata ).getFieldNames( ) );

      List<FieldMetadata> combinedMetadata = new LinkedList<FieldMetadata>( );
      for( List<String> groupOfCombinedFieldNames : this.getCombinedFieldNames( ) )
        {
        Iterator<String> fieldNameToCombineIterator = this.getFieldNamesToCombine( ).iterator( );
        for( String combinedFieldName : groupOfCombinedFieldNames )
          {
          // FIXME: Do this properly: getFieldMetadata should throw an Exception
          String fieldNameToCombine = fieldNameToCombineIterator.next( );
          FieldMetadata currentFieldMetadata = inputMetadata.getFieldMetadata( fieldNameToCombine );
          if( currentFieldMetadata != null )
            {
            FieldMetadata currentCombinedMetadata = currentFieldMetadata.clone( );
            currentCombinedMetadata.setName( combinedFieldName );
            combinedMetadata.add( currentCombinedMetadata );
            }
          else
            {
            combinedMetadata.clear( );
            insertionPoint = fieldMetadata.size( );
            break;
            }
          }
        }

      fieldMetadata.addAll( insertionPoint, combinedMetadata );

      return new RecordMetadata( fieldMetadata );
      }
    else
      {
      this.groupingKeyFieldNames = Collections.emptyList( );
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
    return new CombineTaskWorker( );
    }

  /**
   * @author jgonzalez
   */
  private class CombineTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      if( CombineTask.log.isInfoEnabled( ) )
        {
        String startedMessage = CombineTask.messages.getString( "message.task.started" );
        CombineTask.log.info( MessageFormat.format( startedMessage, CombineTask.this.getName( ) ) );
        }

      InputPort inputPort = CombineTask.this.inputPorts.get( "input" );
      OutputPort outputPort = CombineTask.this.outputPorts.get( "output" );
      List<String> groupingKeyFieldNames = CombineTask.this.groupingKeyFieldNames;

      try
        {
        Record lastKey = null;
        Record currentOutputRecord = null;
        Iterator<List<String>> currentCombinedFieldNamesIterator = CombineTask.this.combinedFieldNames.iterator( );

        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          boolean recordWithSameKeyFound = (lastKey != null) && lastKey.equals( record.extract( groupingKeyFieldNames ) );

          if( recordWithSameKeyFound )
            {
            if( CombineTask.log.isTraceEnabled( ) )
              {
              CombineTask.log.trace( "Combining record into current record: " + record );
              }

            if( !currentCombinedFieldNamesIterator.hasNext( ) )
              {
              outputPort.consume( currentOutputRecord );
              currentOutputRecord = new Record( outputPort.getMetadata( ), true );
              currentOutputRecord.setFields( record.extract( groupingKeyFieldNames ) );

              currentCombinedFieldNamesIterator = CombineTask.this.combinedFieldNames.iterator( );
              }

            Iterator<String> combinedFieldNamesIterator = currentCombinedFieldNamesIterator.next( ).iterator( );
            for( String fieldToCombine : CombineTask.this.fieldNamesToCombine )
              {
              String combinedFieldName = combinedFieldNamesIterator.next( );
              currentOutputRecord.setField( combinedFieldName, record.getField( fieldToCombine ) );
              }
            }
          else
            {
            if( currentOutputRecord != null )
              {
              if( CombineTask.log.isTraceEnabled( ) )
                {
                CombineTask.log.trace( "Outputting combined record: " + currentOutputRecord );
                }

              outputPort.consume( currentOutputRecord );
              }

            if( CombineTask.log.isTraceEnabled( ) )
              {
              CombineTask.log.trace( "Combining record into current record: " + record );
              }

            currentOutputRecord = new Record( outputPort.getMetadata( ), true );
            currentOutputRecord.setFields( record.extract( groupingKeyFieldNames ) );

            currentCombinedFieldNamesIterator = CombineTask.this.combinedFieldNames.iterator( );
            Iterator<String> combinedFieldNamesIterator = currentCombinedFieldNamesIterator.next( ).iterator( );
            for( String fieldToCombine : CombineTask.this.fieldNamesToCombine )
              {
              String combinedFieldName = combinedFieldNamesIterator.next( );
              currentOutputRecord.setField( combinedFieldName, record.getField( fieldToCombine ) );
              }

            lastKey = record.extract( groupingKeyFieldNames );
            }

          Thread.yield( );
          record = inputPort.getRecordQueue( ).take( );
          }

        if( currentOutputRecord != null )
          {
          if( CombineTask.log.isTraceEnabled( ) )
            {
            CombineTask.log.trace( "Outputting combined record: " + currentOutputRecord );
            }

          outputPort.consume( currentOutputRecord );
          }
        }
      catch( InterruptedException exc )
        {
        CombineTask.log.debug( "Exception while reading record", exc );
        }
      finally
        {
        outputPort.consume( Record.END_OF_DATA );

        if( CombineTask.log.isInfoEnabled( ) )
          {
          String finishedMessage = CombineTask.messages.getString( "message.task.finished" );
          CombineTask.log.info( MessageFormat.format( finishedMessage, CombineTask.this.getName( ) ) );
          }
        }
      }
    }
  }
