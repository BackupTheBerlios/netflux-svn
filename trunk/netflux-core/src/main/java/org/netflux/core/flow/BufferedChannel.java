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
package org.netflux.core.flow;

import java.util.ResourceBundle;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.InvalidRecordMetadataException;
import org.netflux.core.Record;

/**
 * A channel using an internal queue as a intermediate buffer between its input and output.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class BufferedChannel extends SimpleChannel
  {
  private static Log            log      = LogFactory.getLog( BufferedChannel.class );
  private static ResourceBundle messages = ResourceBundle.getBundle( BufferedChannel.class.getName( ) );

  private BlockingQueue<Record> buffer;
  private NotifierThread        notifier;

  /**
   * Creates a new <code>BufferedChannel</code>.
   */
  public BufferedChannel( )
    {
    this( "BufferedChannel|" + UUID.randomUUID( ) );
    }

  /**
   * Creates a new <code>BufferedChannel</code> with the specified <code>name</code>.
   * 
   * @param name the name of the buffered channel
   */
  public BufferedChannel( String name )
    {
    super( name );
    this.buffer = new LinkedBlockingQueue<Record>( );
    this.notifier = new BufferedChannel.NotifierThread( );
    this.notifier.start( );
    }

  /**
   * Creates a new <code>BufferedChannel</code> with the specified initial <code>capacity</code> in its internal queue.
   * 
   * @param capacity the initial capacity of the internal queue.
   */
  public BufferedChannel( int capacity )
    {
    this( "BufferedChannel|" + UUID.randomUUID( ), capacity );
    }

  /**
   * Creates a new <code>BufferedChannel</code> with the specified <code>name</code> and initial <code>capacity</code> in its
   * internal queue.
   * 
   * @param name the name of the buffered channel
   * @param capacity the initial capacity of the internal queue.
   */
  public BufferedChannel( String name, int capacity )
    {
    super( name );
    this.buffer = new LinkedBlockingQueue<Record>( capacity );
    this.notifier = new BufferedChannel.NotifierThread( );
    this.notifier.start( );
    }

  @Override
  public void setName( String name )
    {
    super.setName( name );
    this.notifier.setName( name );
    }

  /**
   * Consumption implementation that internally stores the received <code>record</code> in the internal queue. The
   * <code>record</code> will be retrieved and passed to the registered <code>RecordSink</code>s later by an internal thread.
   */
  @Override
  public void consume( Record record )
    {
    if( record.getMetadata( ).equals( this.getMetadata( ) ) || record.equals( Record.END_OF_DATA ) )
      {
      try
        {
        if( BufferedChannel.log.isTraceEnabled( ) ) BufferedChannel.log.trace( "Storing record in internal queue: " + record );
        this.buffer.put( record );
        }
      catch( InterruptedException exc )
        {
        BufferedChannel.log.debug( "Interrupted while putting record in queue", exc );
        this.notifier.errorDetected = true;
        this.notifier.interrupt( );
        }
      }
    else
      {
      if( BufferedChannel.log.isInfoEnabled( ) )
        {
        BufferedChannel.log.info( BufferedChannel.messages.getString( "exception.invalid.metadata" ) );
        }
      throw new InvalidRecordMetadataException( record, this.getMetadata( ) );
      }
    }

  /**
   * Execution thread used to deliver records taken from the internal queue to the registered <code>RecordSink</code>s.
   * 
   * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
   */
  private class NotifierThread extends Thread
    {
    private boolean errorDetected = false;

    public NotifierThread( )
      {
      super( BufferedChannel.this.getName( ) );
      }

    // TODO: This is a very naive implementation of this stuff
    @Override
    public void run( )
      {
      try
        {
        // TODO: Change starting and finished messages to INFO level
        BufferedChannel.log.debug( "Starting record processing" );

        Record record = BufferedChannel.this.buffer.take( );
        while( !record.equals( Record.END_OF_DATA ) && !this.errorDetected )
          {
          if( BufferedChannel.log.isTraceEnabled( ) ) BufferedChannel.log.trace( "Record retrieved from internal queue: " + record );
          BufferedChannel.super.consume( record );
          record = BufferedChannel.this.buffer.take( );
          }
        }
      catch( InterruptedException exc )
        {
        BufferedChannel.log.debug( "Interrupted while consuming record in queue", exc );
        }

      BufferedChannel.super.consume( Record.END_OF_DATA );
      BufferedChannel.log.debug( "Finished record processing" );
      }
    }
  }
