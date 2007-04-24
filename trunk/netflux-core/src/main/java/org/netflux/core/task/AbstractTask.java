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
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;
import org.netflux.core.Task;
import org.netflux.core.flow.InputPort;
import org.netflux.core.flow.OutputPort;
import org.netflux.core.flow.SimpleInputPort;
import org.netflux.core.flow.SimpleOutputPort;

/**
 * <p>
 * This class provides a skeletal implementation of the {@link org.netflux.core.Task} interface, to minimize the effort required to
 * implement this interface.
 * </p>
 * <p>
 * To implement a new task, the programmer should extend this class and provide implementations for the two abstract methods defined in
 * this class: {@link #computeMetadata(String, InputPort, RecordMetadata)} and {@link #getTaskWorker()}. The
 * {@link #computeMetadata(String, InputPort, RecordMetadata)} method is called every time there is some change in the metadata of one
 * of the defined input ports, so the metadata of the output ports must be computed based on the new metadata. The
 * {@link #getTaskWorker()} method should return a {@link java.lang.Thread} that once started takes all the records provided in the
 * input ports and, after some processing, writes them to the corresponding output records. The provided thread <b>should never modify
 * input records</b>, as those records may have been handled to other tasks for parallel processing, and modifying them would cause
 * improper processing or even failure of other tasks in the same process.
 * </p>
 * <p>
 * The programmer should generally provide a constructor that should call one of the provided constructors with a set of names of the
 * input and output ports (generally fixed for a given task) and an optional task name (highly advisable for logging and thread
 * identification purposes), so those ports may be properly initialized upon task construction. Child classes should also contain
 * getters and setters for any property that may be used to customize the behaviour of the task.
 * </p>
 * <p>
 * It may be also convenient to add getters and setters to provide direct access to input and output ports. This is, if the task has a
 * port named output, it should be convenient to provide a <code>getOutputPort</code> method that would directly return the port
 * named output. This would make easier using a task by means of a dependency injection framework.
 * </p>
 * <p>
 * Here you have a very simple example of a task that simply forwards every record received through its input port to its output port:
 * </p>
 * 
 * <pre>
 * public class CopyTask extends AbstractTask
 *   {
 *   public CopyTask( )
 *     {
 *     super( Collections.singleton( &quot;input&quot; ), Collections.singleton( &quot;output&quot; ) );
 *     }
 * 
 *   protected RecordMetadata computeMetadata( String outputPortName, InputPort changedInputPort, RecordMetadata newMetadata )
 *     {
 *     return newMetadata;
 *     }
 * 
 *   protected Thread getTaskWorker( )
 *     {
 *     return new CopyTaskWorker( );
 *     }
 * 
 *   private class CopyTaskWorker extends Thread
 *     {
 *     public void run( )
 *       {
 *       InputPort inputPort = CopyTask.this.inputPorts.get( &quot;input&quot; );
 *       OutputPort outputPort = CopyTask.this.outputPorts.get( &quot;output&quot; );
 *       try
 *         {
 *         Record record = inputPort.getRecordQueue( ).take( );
 *         while( !record.equals( Record.END_OF_DATA ) )
 *           {
 *           outputPort.consume( record );
 *           record = inputPort.getRecordQueue( ).take( );
 *           }
 *         }
 *       catch( InterruptedException exc )
 *         {
 *         exc.printStackTrace( );
 *         }
 *       finally
 *         {
 *         outputPort.consume( Record.END_OF_DATA );
 *         }
 *       }
 *     }
 *   }
 * </pre>
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public abstract class AbstractTask implements Task
  {
  private static Log                log         = LogFactory.getLog( AbstractTask.class );
  private static ResourceBundle     messages    = ResourceBundle.getBundle( AbstractTask.class.getName( ) );

  protected Map<String, InputPort>  inputPorts  = new HashMap<String, InputPort>( );
  protected Map<String, OutputPort> outputPorts = new HashMap<String, OutputPort>( );
  private String                    name;

  /**
   * Creates a new task and initializes its input and output ports using the given names.
   * 
   * @param inputPortNames the name of the input ports this task will use to read records.
   * @param outputPortNames the name of the output ports this task will use to write records.
   */
  protected AbstractTask( Set<String> inputPortNames, Set<String> outputPortNames )
    {
    this( "Task|" + UUID.randomUUID( ).toString( ), inputPortNames, outputPortNames );
    }

  /**
   * Creates a new task with the given name, and initializes its input and output ports using the given names.
   * 
   * @param name the name of the task to be created.
   * @param inputPortNames the name of the input ports this task will use to read records.
   * @param outputPortNames the name of the output ports this task will use to write records.
   */
  protected AbstractTask( String name, Set<String> inputPortNames, Set<String> outputPortNames )
    {
    this.name = name;
    Iterator<String> inputPortNamesIterator = inputPortNames.iterator( );
    while( inputPortNamesIterator.hasNext( ) )
      {
      String inputPortName = inputPortNamesIterator.next( );
      InputPort currentInputPort = new SimpleInputPort( this.name + ":" + inputPortName );
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
      this.outputPorts.put( outputPortName, new SimpleOutputPort( this.name + ":" + outputPortName ) );
      }
    }

  public String getName( )
    {
    return this.name;
    }

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

  public Set<String> getInputPortNames( )
    {
    return this.inputPorts.keySet( );
    }

  public Map<String, RecordSink> getInputPorts( )
    {
    // TODO: Is this correct or should I use a bounded wildcard in the return type?
    return new HashMap<String, RecordSink>( this.inputPorts );
    }

  public RecordSink getInputPort( String portName )
    {
    return this.inputPorts.get( portName );
    }

  public Set<String> getOutputPortNames( )
    {
    return this.outputPorts.keySet( );
    }

  public Map<String, RecordSource> getOutputPorts( )
    {
    // TODO: Is this correct or should I use a bounded wildcard in the return type?
    return new HashMap<String, RecordSource>( this.outputPorts );
    }

  public RecordSource getOutputPort( String portName )
    {
    return this.outputPorts.get( portName );
    }

  public void start( )
    {
    // TODO: Thread handling
    // TODO: Improve uncaught exception handling
    Thread taskWorker = this.getTaskWorker( );
    taskWorker.setName( this.getName( ) );
    taskWorker.setUncaughtExceptionHandler( new Thread.UncaughtExceptionHandler( )
      {
        public void uncaughtException( Thread thread, Throwable throwable )
          {
          String message = MessageFormat.format( AbstractTask.messages.getString( "exception.uncaught.exception" ), thread.getName( ) );
          AbstractTask.log.error( message, throwable );
          for( OutputPort outputPort : AbstractTask.this.outputPorts.values( ) )
            {
            outputPort.consume( Record.END_OF_DATA );
            }
          }
      } );
    taskWorker.start( );
    }

  /**
   * Listener method invoked whenever there is a change in the metadata of any input port, responsible for updating the metadata of
   * output ports. This method iterates over the set of output ports, setting their metadata from the result of calling the
   * {@link #computeMetadata(String, InputPort, RecordMetadata)} method. Child classes should override this method only when the
   * default behaviour is not suitable, in other cases overriding the {@link #computeMetadata(String, InputPort, RecordMetadata)}
   * method should be enough.
   * 
   * @param inputPort the input port which metadata has changed.
   * @param newMetadata the new metadata of the provided input port.
   */
  protected void updateMetadata( InputPort inputPort, RecordMetadata newMetadata )
    {
    for( Map.Entry<String, OutputPort> outputPortEntry : this.outputPorts.entrySet( ) )
      {
      outputPortEntry.getValue( ).setMetadata( this.computeMetadata( outputPortEntry.getKey( ), inputPort, newMetadata ) );
      }
    }

  /**
   * Computes the metadata to be associated to a given output port based on the changes in the provided input port. Child classes
   * should override this method, and return the metadata that should be associated with the output port with the given
   * <code>outputPortName</code>, knowing that the input port <code>changedInputPort</code> has changed its metadata to
   * <code>newMetadata</code>. The provided method could safely ignore these last two parameters, as they are just provided in case
   * there could be some optimization in metadata computation based on them.
   * 
   * @param outputPortName the name of the output port which metadata should be returned.
   * @param changedInputPort the name of the input port which metadata has changed.
   * @param newMetadata the new metadata associated with the given input port.
   * @return the new metadata to be associated with the output port with name <code>outputPortName</code>.
   */
  protected abstract RecordMetadata computeMetadata( String outputPortName, InputPort changedInputPort, RecordMetadata newMetadata );

  /**
   * Returns the thread resposible for processing the input records and producing the corresponding output records. The provided thread
   * should read records from the taks' input ports, and after processing, should write the resulting records to the corresponding
   * output ports. The provided thread <b>should never modify input records</b>, as those records may have been handled to other tasks
   * for parallel processing, and modifying them would cause improper processing or even failure of other tasks in the same process.
   * 
   * @return the thread responsible for the processing to be done by this task.
   */
  protected abstract Thread getTaskWorker( );
  }
