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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.netflux.core.Record;

/**
 * @author jgonzalez
 */
public class BufferedChannel extends SimpleChannel
  {
  protected BlockingQueue<Record> buffer;
  protected Thread                notifier;

  /**
   * 
   */
  public BufferedChannel( )
    {
    this.buffer = new LinkedBlockingQueue<Record>( );
    this.notifier = new BufferedChannel.NotifierThread( );
    this.notifier.start( );
    }

  /**
   * @param capacity
   */
  public BufferedChannel( int capacity )
    {
    this.buffer = new LinkedBlockingQueue<Record>( capacity );
    this.notifier = new BufferedChannel.NotifierThread( );
    this.notifier.start( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.flow.SimpleChannel#consume(java.lang.Object)
   */
  @Override
  public void consume( Record object )
    {
    try
      {
      this.buffer.put( object );
      }
    catch( InterruptedException exc )
      {
      // TODO Auto-generated catch block
      exc.printStackTrace( );
      }
    }

  /**
   * @author jgonzalez
   */
  protected class NotifierThread extends Thread
    {
    // TODO: This is a very naive implementation of this stuff
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run( )
      {
      try
        {
        Record record;
        do
          {
          record = BufferedChannel.this.buffer.take( );
          BufferedChannel.super.consume( record );
          }
        while( !record.equals( Record.END_OF_DATA ) );
        }
      catch( InterruptedException exc )
        {
        // TODO: handle exception
        exc.printStackTrace( );
        }
      }
    }
  }
