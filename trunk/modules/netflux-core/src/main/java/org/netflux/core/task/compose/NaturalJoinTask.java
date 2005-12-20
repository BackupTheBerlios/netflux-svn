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
import java.util.Set;

import org.netflux.core.Channel;
import org.netflux.core.FieldMetadata;
import org.netflux.core.InputPort;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.task.AbstractTask;

// TODO: Error handling
/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class NaturalJoinTask extends AbstractTask
  {
  private static final Set<String> INPUT_PORT_NAMES  = new HashSet<String>( Arrays.asList( new String[] {"leftInput", "rightInput"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  public enum NaturalJoinType
    {
    INNER_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN, FULL_OUTER_JOIN
    }

  private NaturalJoinTask.NaturalJoinType joinType = NaturalJoinTask.NaturalJoinType.INNER_JOIN;
  protected List<String>                  joinKeyFieldNames;

  /**
   * 
   */
  public NaturalJoinTask( )
    {
    super( NaturalJoinTask.INPUT_PORT_NAMES, NaturalJoinTask.OUTPUT_PORT_NAMES );
    }

  /**
   * @return Returns the joinType.
   */
  public NaturalJoinTask.NaturalJoinType getJoinType( )
    {
    return this.joinType;
    }

  /**
   * @param joinType The joinType to set.
   */
  public void setJoinType( NaturalJoinTask.NaturalJoinType joinType )
    {
    this.joinType = joinType;
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
    // TODO: Handle errors !!!!!
    RecordMetadata leftInputMetadata = this.inputPorts.get( "leftInput" ).getMetadata( );
    RecordMetadata rightInputMetadata = this.inputPorts.get( "rightInput" ).getMetadata( );

    if( leftInputMetadata != null && rightInputMetadata != null )
      {
      this.joinKeyFieldNames = new LinkedList<String>( leftInputMetadata.getFieldNames( ) );
      this.joinKeyFieldNames.retainAll( rightInputMetadata.getFieldNames( ) );

      List<FieldMetadata> fieldMetadata = new LinkedList<FieldMetadata>( leftInputMetadata.getFieldMetadata( ) );
      List<FieldMetadata> rightFieldMetadata = new LinkedList<FieldMetadata>( rightInputMetadata.getFieldMetadata( ) );
      Iterator<FieldMetadata> rightFieldMetadataIterator = rightFieldMetadata.iterator( );
      while( rightFieldMetadataIterator.hasNext( ) )
        {
        FieldMetadata currentFieldMetadata = rightFieldMetadataIterator.next( );
        if( this.joinKeyFieldNames.contains( currentFieldMetadata.getName( ) ) )
          {
          rightFieldMetadataIterator.remove( );
          }
        }
      fieldMetadata.addAll( rightFieldMetadata );

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
  public RecordSink getLeftInputPort( )
    {
    return this.getInputPort( "leftInput" );
    }

  /**
   * @return
   */
  public RecordSink getRightInputPort( )
    {
    return this.getInputPort( "rightInput" );
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
    return new NaturalJoinTaskWorker( );
    }

  /**
   * @author jgonzalez
   */
  private class NaturalJoinTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      InputPort leftInputPort = NaturalJoinTask.this.inputPorts.get( "leftInput" );
      InputPort rightInputPort = NaturalJoinTask.this.inputPorts.get( "rightInput" );
      Channel outputPort = NaturalJoinTask.this.outputPorts.get( "output" );

      // Precomputed for speed
      RecordMetadata rightMetadataWithoutKey = rightInputPort.getMetadata( ).supress( NaturalJoinTask.this.joinKeyFieldNames );

      try
        {
        List<Record> leftRecords = new LinkedList<Record>( );
        Record lastLeftRecord = leftInputPort.getRecordQueue( ).take( );
        List<Record> rightRecords = new LinkedList<Record>( );
        Record lastRightRecord = rightInputPort.getRecordQueue( ).take( );

        while( !lastLeftRecord.equals( Record.END_OF_DATA ) || !lastRightRecord.equals( Record.END_OF_DATA ) )
          {
          if( !lastLeftRecord.equals( Record.END_OF_DATA ) && leftRecords.isEmpty( ) )
            {
            if( lastLeftRecord != null ) leftRecords.add( lastLeftRecord );
            Record inputRecord = leftInputPort.getRecordQueue( ).take( );
            while( !inputRecord.equals( Record.END_OF_DATA )
                && lastLeftRecord.extract( NaturalJoinTask.this.joinKeyFieldNames ).equals(
                    inputRecord.extract( NaturalJoinTask.this.joinKeyFieldNames ) ) )
              {
              leftRecords.add( inputRecord );
              Thread.yield( );
              inputRecord = leftInputPort.getRecordQueue( ).take( );
              }

            lastLeftRecord = inputRecord;
            }
          if( !lastRightRecord.equals( Record.END_OF_DATA ) && rightRecords.isEmpty( ) )
            {
            if( lastRightRecord != null ) rightRecords.add( lastRightRecord );
            Record inputRecord = rightInputPort.getRecordQueue( ).take( );
            while( !inputRecord.equals( Record.END_OF_DATA )
                && lastRightRecord.extract( NaturalJoinTask.this.joinKeyFieldNames ).equals(
                    inputRecord.extract( NaturalJoinTask.this.joinKeyFieldNames ) ) )
              {
              rightRecords.add( inputRecord );
              Thread.yield( );
              inputRecord = rightInputPort.getRecordQueue( ).take( );
              }

            lastRightRecord = inputRecord;
            }

          int comparison;
          if( leftRecords.isEmpty( ) )
            {
            comparison = 1;
            }
          else if( rightRecords.isEmpty( ) )
            {
            comparison = -1;
            }
          else
            {
            comparison = leftRecords.get( 0 ).extract( NaturalJoinTask.this.joinKeyFieldNames ).compareTo(
                rightRecords.get( 0 ).extract( NaturalJoinTask.this.joinKeyFieldNames ) );
            }

          if( comparison == 0 )
            {
            for( Record leftRecord : leftRecords )
              {
              for( Record rightRecord : rightRecords )
                {
                Record joinedRecord = leftRecord.concatenate( rightRecord.supress( NaturalJoinTask.this.joinKeyFieldNames ) );
                outputPort.consume( joinedRecord );
                }
              }
            leftRecords.clear( );
            rightRecords.clear( );
            }
          else if( comparison < 0 )
            {
            if( NaturalJoinTask.this.getJoinType( ).equals( NaturalJoinTask.NaturalJoinType.LEFT_OUTER_JOIN )
                || NaturalJoinTask.this.getJoinType( ).equals( NaturalJoinTask.NaturalJoinType.FULL_OUTER_JOIN ) )
              {
              for( Record leftRecord : leftRecords )
                {
                Record nullRightRecord = new Record( rightMetadataWithoutKey, true );
                Record joinedRecord = leftRecord.concatenate( nullRightRecord );
                outputPort.consume( joinedRecord );
                }
              }
            leftRecords.clear( );
            }
          else
            {
            // comparison > 0
            if( NaturalJoinTask.this.getJoinType( ).equals( NaturalJoinTask.NaturalJoinType.RIGHT_OUTER_JOIN )
                || NaturalJoinTask.this.getJoinType( ).equals( NaturalJoinTask.NaturalJoinType.FULL_OUTER_JOIN ) )
              {
              for( Record rightRecord : rightRecords )
                {
                Record joinedRecord = new Record( leftInputPort.getMetadata( ), true );
                joinedRecord.setFields( rightRecord.extract( NaturalJoinTask.this.joinKeyFieldNames ) );
                joinedRecord.add( rightRecord.supress( NaturalJoinTask.this.joinKeyFieldNames ) );
                outputPort.consume( joinedRecord );
                }
              }
            rightRecords.clear( );
            }
          }

        outputPort.consume( Record.END_OF_DATA );
        }
      catch( InterruptedException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      }
    }
  }
