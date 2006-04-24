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
package org.netflux.core.task;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.Task;
import org.netflux.core.flow.InputPort;
import org.netflux.core.flow.OutputPort;
import org.netflux.core.flow.SimpleInputPort;
import org.netflux.core.flow.SimpleOutputPort;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public abstract class AbstractTask implements Task
  {
  protected Map<String, InputPort>  inputPorts  = new HashMap<String, InputPort>( );
  protected Map<String, OutputPort> outputPorts = new HashMap<String, OutputPort>( );
  private String                    name;

  protected AbstractTask( Set<String> inputPortNames, Set<String> outputPortNames )
    {
    this( "Task|" + UUID.randomUUID( ).toString( ), inputPortNames, outputPortNames );
    }

  protected AbstractTask( String name, Set<String> inputPortNames, Set<String> outputPortNames )
    {
    this.name = name;
    Iterator<String> inputPortNamesIterator = inputPortNames.iterator( );
    while( inputPortNamesIterator.hasNext( ) )
      {
      String inputPortName = inputPortNamesIterator.next( );
      InputPort currentInputPort = new SimpleInputPort( );
      this.inputPorts.put( inputPortName, currentInputPort );
      currentInputPort.addPropertyChangeListener( "metadata", new PropertyChangeListener( )
        {
          public void propertyChange( PropertyChangeEvent event )
            {
            AbstractTask.this.updateMetadata( (InputPort) event.getSource( ), (RecordMetadata) event.getNewValue( ) );
            }
        } );
      }

    Iterator<String> outputPortNamesIterator = outputPortNames.iterator( );
    while( outputPortNamesIterator.hasNext( ) )
      {
      String outputPortName = outputPortNamesIterator.next( );
      this.outputPorts.put( outputPortName, new SimpleOutputPort( ) );
      }
    }

  public String getName( )
    {
    return this.name;
    }

  /**
   * Sets the name of this task.
   * 
   * @param name the new name of the task
   */
  public void setName( String name )
    {
    this.name = name;
    for( Map.Entry<String, InputPort> inputPortEntry : this.inputPorts.entrySet( ) )
      {
      inputPortEntry.getValue( ).setName( this.getName( ) + ":" + inputPortEntry.getKey( ) );
      }
    for( Map.Entry<String, OutputPort> outputPortEntry : this.outputPorts.entrySet( ) )
      {
      outputPortEntry.getValue( ).setName( this.getName( ) + ":" + outputPortEntry.getKey( ) );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSink#getInputPortNames()
   */
  public Set<String> getInputPortNames( )
    {
    return this.inputPorts.keySet( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSink#getInputPorts()
   */
  public Map<String, RecordSink> getInputPorts( )
    {
    // TODO: Is this correct or should I use a bounded wildcard in the return type?
    return new HashMap<String, RecordSink>( this.inputPorts );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSink#getInputPort(java.lang.String)
   */
  public RecordSink getInputPort( String portName )
    {
    return this.inputPorts.get( portName );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#getOutputPortNames()
   */
  public Set<String> getOutputPortNames( )
    {
    return this.outputPorts.keySet( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#getOutputPorts()
   */
  public Map<String, RecordSource> getOutputPorts( )
    {
    // TODO: Is this correct or should I use a bounded wildcard in the return type?
    return new HashMap<String, RecordSource>( this.outputPorts );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#getOutputPort(java.lang.String)
   */
  public RecordSource getOutputPort( String portName )
    {
    return this.outputPorts.get( portName );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.DataSource#start()
   */
  public void start( )
    {
    // TODO: Thread handling
    // FIXME: Catch exceptions thrown from the thread (using join? how?) and send END_OF_DATA to all active output ports
    Thread taskWorker = this.getTaskWorker( );
    taskWorker.setName( this.getName( ) );
    taskWorker.start( );
    }

  /**
   * @param inputPort
   * @param newMetadata
   */
  protected void updateMetadata( InputPort inputPort, RecordMetadata newMetadata )
    {
    for( Map.Entry<String, OutputPort> outputPortEntry : this.outputPorts.entrySet( ) )
      {
      outputPortEntry.getValue( ).setMetadata( this.computeMetadata( outputPortEntry.getKey( ), inputPort, newMetadata ) );
      }
    }

  /**
   * @param outputPortName
   * @param changedInputPort
   * @param newMetadata
   * @return
   */
  protected abstract RecordMetadata computeMetadata( String outputPortName, InputPort changedInputPort, RecordMetadata newMetadata );

  /**
   * @return
   */
  protected abstract Thread getTaskWorker( );
  }
