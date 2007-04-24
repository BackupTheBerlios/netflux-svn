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

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.flow.InputPort;
import org.netflux.core.flow.OutputPort;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class DiscriminatedCombineTask extends CombineTask
  {
  private static Log            log                 = LogFactory.getLog( DiscriminatedCombineTask.class );
  private static ResourceBundle messages            = ResourceBundle.getBundle( DiscriminatedCombineTask.class.getName( ) );

  protected String              discriminatorFieldName;
  protected List<Object>        discriminatorValues = new LinkedList<Object>( );

  /**
   * Creates a new discriminated combine task.
   */
  public DiscriminatedCombineTask( )
    {
    super( );
    }

  /**
   * Creates a new discriminated combine task with the provided name.
   * 
   * @param name the name of the new discriminated combine task.
   */
  public DiscriminatedCombineTask( String name )
    {
    super( name );
    }

  /**
   * @return Returns the discriminatorFieldName.
   */
  public String getDiscriminatorFieldName( )
    {
    return this.discriminatorFieldName;
    }

  /**
   * @param discriminatorFieldName The discriminatorFieldName to set.
   */
  public void setDiscriminatorFieldName( String discriminatorFieldName )
    {
    this.discriminatorFieldName = discriminatorFieldName;
    }

  /**
   * @return Returns the discriminatorValues.
   */
  public List<Object> getDiscriminatorValues( )
    {
    return Collections.unmodifiableList( this.discriminatorValues );
    }

  /**
   * @param discriminatorValues The discriminatorValues to set.
   */
  public void setDiscriminatorValues( List<Object> discriminatorValues )
    {
    this.discriminatorValues = new ArrayList<Object>( discriminatorValues );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.compose.CombineTask#computeMetadata(java.lang.String, org.netflux.core.InputPort,
   *      org.netflux.core.RecordMetadata)
   */
  @Override
  protected RecordMetadata computeMetadata( String outputPortName, InputPort changedInputPort, RecordMetadata newMetadata )
    {
    RecordMetadata superMetadata = super.computeMetadata( outputPortName, changedInputPort, newMetadata );
    this.groupingKeyFieldNames.remove( this.getDiscriminatorFieldName( ) );
    return superMetadata.supress( Collections.singletonList( this.getDiscriminatorFieldName( ) ) );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.compose.CombineTask#getTaskWorker()
   */
  @Override
  protected Thread getTaskWorker( )
    {
    return new DiscriminatedCombineTaskWorker( );
    }

  /**
   * @author jgonzalez
   */
  private class DiscriminatedCombineTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      if( DiscriminatedCombineTask.log.isInfoEnabled( ) )
        {
        String startedMessage = DiscriminatedCombineTask.messages.getString( "message.task.started" );
        DiscriminatedCombineTask.log.info( MessageFormat.format( startedMessage, DiscriminatedCombineTask.this.getName( ) ) );
        }

      InputPort inputPort = DiscriminatedCombineTask.this.inputPorts.get( "input" );
      OutputPort outputPort = DiscriminatedCombineTask.this.outputPorts.get( "output" );
      List<String> groupingKeyFieldNames = DiscriminatedCombineTask.this.groupingKeyFieldNames;

      try
        {
        Record lastKey = null;
        Record currentOutputRecord = null;

        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          boolean recordWithSameKeyFound = (lastKey != null) && lastKey.equals( record.extract( groupingKeyFieldNames ) );

          if( !recordWithSameKeyFound )
            {
            if( currentOutputRecord != null )
              {
              if( DiscriminatedCombineTask.log.isTraceEnabled( ) )
                {
                DiscriminatedCombineTask.log.trace( "Outputting combined record: " + currentOutputRecord );
                }

              outputPort.consume( currentOutputRecord );
              }

            currentOutputRecord = new Record( outputPort.getMetadata( ), true );
            currentOutputRecord.setFields( record.extract( groupingKeyFieldNames ) );

            lastKey = record.extract( groupingKeyFieldNames );
            }

          if( DiscriminatedCombineTask.log.isTraceEnabled( ) )
            {
            DiscriminatedCombineTask.log.trace( "Combining record into current record: " + record );
            }

          Object discriminatorValue = record.getValue( Serializable.class, DiscriminatedCombineTask.this.getDiscriminatorFieldName( ) );
          int discriminatorIndex = DiscriminatedCombineTask.this.getDiscriminatorValues( ).indexOf( discriminatorValue );
          List<String> currentCombinedFieldNames = DiscriminatedCombineTask.this.getCombinedFieldNames( ).get( discriminatorIndex );
          Iterator<String> combinedFieldNamesIterator = currentCombinedFieldNames.iterator( );
          for( String fieldToCombine : DiscriminatedCombineTask.this.fieldNamesToCombine )
            {
            String combinedFieldName = combinedFieldNamesIterator.next( );
            currentOutputRecord.setField( combinedFieldName, record.getField( fieldToCombine ) );
            }

          Thread.yield( );
          record = inputPort.getRecordQueue( ).take( );
          }

        if( currentOutputRecord != null )
          {
          if( DiscriminatedCombineTask.log.isTraceEnabled( ) )
            {
            DiscriminatedCombineTask.log.trace( "Outputting combined record: " + currentOutputRecord );
            }

          outputPort.consume( currentOutputRecord );
          }
        }
      catch( InterruptedException exc )
        {
        DiscriminatedCombineTask.log.debug( "Exception while reading record", exc );
        }
      finally
        {
        outputPort.consume( Record.END_OF_DATA );

        if( DiscriminatedCombineTask.log.isInfoEnabled( ) )
          {
          String finishedMessage = DiscriminatedCombineTask.messages.getString( "message.task.finished" );
          DiscriminatedCombineTask.log.info( MessageFormat.format( finishedMessage, DiscriminatedCombineTask.this.getName( ) ) );
          }
        }
      }
    }
  }
