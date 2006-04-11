package org.netflux.core.util;

import java.beans.PropertyChangeSupport;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;

public class RecordSourceSupport
  {
  private static Log            log         = LogFactory.getLog( RecordSourceSupport.class );

  private String                name;
  private RecordSource          recordSource;
  private Set<RecordSink>       recordSinks = new HashSet<RecordSink>( );
  private PropertyChangeSupport propertyChangeSupport;

  public RecordSourceSupport( String name, RecordSource recordSource, PropertyChangeSupport propertyChangeSupport )
    {
    this.name = name;
    this.recordSource = recordSource;
    this.propertyChangeSupport = propertyChangeSupport;
    }

  public String getName( )
    {
    return name;
    }

  public void setName( String name )
    {
    this.name = name;
    }

  public void addRecordSink( RecordSink recordSink )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Adding new record sink" );
    synchronized( this.recordSinks )
      {
      if( !this.recordSinks.contains( recordSink ) )
        {
        this.recordSinks.add( recordSink );
        recordSink.setRecordSource( this.recordSource );
        }
      }
    }

  public void removeRecordSink( RecordSink recordSink )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Removing record sink" );
    synchronized( this.recordSinks )
      {
      if( recordSink.getRecordSource( ) == this )
        {
        recordSink.setRecordSource( null );
        }
      this.recordSinks.remove( recordSink );
      }
    }

  public Set<RecordSink> getRecordSinks( )
    {
    return Collections.unmodifiableSet( this.recordSinks );
    }

  public void setRecordSinks( Set<RecordSink> recordSinks )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Setting record sinks" );
    synchronized( this.recordSinks )
      {
      this.recordSinks.clear( );
      this.recordSinks.addAll( recordSinks );
      for( RecordSink recordSink : recordSinks )
        {
        recordSink.setRecordSource( this.recordSource );
        }
      }
    }

  public void setRecordSink( RecordSink recordSink )
    {
    RecordSourceSupport.log.debug( this.getName( ) + " - Setting record sink" );
    this.setRecordSinks( Collections.singleton( recordSink ) );
    }
  }
