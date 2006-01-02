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
package org.netflux.core.task.transform;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class TransformationTask extends AbstractTask
  {
  private static final Set<String> INPUT_PORT_NAMES  = new HashSet<String>( Arrays.asList( new String[] {"input"} ) );
  private static final Set<String> OUTPUT_PORT_NAMES = new HashSet<String>( Arrays.asList( new String[] {"output"} ) );

  protected List<Transformation>   transformations   = new LinkedList<Transformation>( );

  /**
   * 
   */
  public TransformationTask( )
    {
    super( TransformationTask.INPUT_PORT_NAMES, TransformationTask.OUTPUT_PORT_NAMES );
    }

  /**
   * @return Returns the transformations.
   */
  public List<Transformation> getTransformations( )
    {
    return Collections.unmodifiableList( this.transformations );
    }

  /**
   * @param transformations The transformations to set.
   */
  public void setTransformations( List<Transformation> transformations )
    {
    this.transformations.clear( );
    this.transformations.addAll( transformations );

    this.updateMetadata( null, null );
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
    List<FieldMetadata> metadataList = new LinkedList<FieldMetadata>( );
    for( Transformation transformation : this.transformations )
      {
      transformation.setInputMedatata( this.inputPorts.get( "input" ).getMetadata( ) );
      metadataList.addAll( transformation.getOutputMetadata( ).getFieldMetadata( ) );
      }

    return new RecordMetadata( metadataList );
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
    return new TransformationTaskWorker( );
    }

  /**
   * @author jgonzalez
   */
  private class TransformationTaskWorker extends Thread
    {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      InputPort inputPort = TransformationTask.this.inputPorts.get( "input" );
      Channel outputPort = TransformationTask.this.outputPorts.get( "output" );
      try
        {
        Record record = inputPort.getRecordQueue( ).take( );
        while( !record.equals( Record.END_OF_DATA ) )
          {
          Record transformedRecord = new Record( outputPort.getMetadata( ) );
          for( Transformation transformation : TransformationTask.this.transformations )
            {
            Record partiallyTransformedRecord = transformation.transform( record );
            for( FieldMetadata fieldMetadata : partiallyTransformedRecord.getMetadata( ).getFieldMetadata( ) )
              {
              transformedRecord.setField( fieldMetadata.getName( ), partiallyTransformedRecord.getField( fieldMetadata.getName( ) ) );
              }
            }
          outputPort.consume( transformedRecord );

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
