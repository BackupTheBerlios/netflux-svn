package org.netflux.core.util;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;
import org.netflux.core.RecordSource;

public class RecordSinkSupport
  {
  private static Log               log                  = LogFactory.getLog( RecordSinkSupport.class );

  private String                   name;
  private RecordSink               recordSink;
  private RecordSource             recordSource;
  private RecordMetadata           metadata;
  private PropertyChangeSupport    propertyChangeSupport;
  protected PropertyChangeListener recordSourceListener = new PropertyChangeListener( )
                                                          {
                                                            public void propertyChange( PropertyChangeEvent event )
                                                              {
                                                              RecordSinkSupport.this
                                                                  .setMetadata( (RecordMetadata) event.getNewValue( ) );
                                                              }
                                                          };

  public RecordSinkSupport( String name, RecordSink recordSink, PropertyChangeSupport propertyChangeSupport )
    {
    this.name = name;
    this.recordSink = recordSink;
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

  public RecordSource getRecordSource( )
    {
    return this.recordSource;
    }

  public void setRecordSource( RecordSource recordSource )
    {
    if( this.recordSource != recordSource )
      {
      RecordSinkSupport.log.debug( this.getName( ) + " - Setting new record source" );
      if( this.recordSource != null )
        {
        RecordSinkSupport.log.trace( this.getName( ) + " - Removing property change listener of prior record source" );
        this.recordSource.removePropertyChangeListener( "metadata", this.recordSourceListener );
        this.recordSource.removeRecordSink( this.recordSink );
        }

      RecordSource oldRecordSource = this.recordSource;
      this.recordSource = recordSource;
      if( this.recordSource != null )
        {
        this.setMetadata( this.recordSource.getMetadata( ) );
        RecordSinkSupport.log.trace( this.getName( ) + " - Setting property change listener for new record source" );
        this.recordSource.addRecordSink( this.recordSink );
        this.recordSource.addPropertyChangeListener( "metadata", this.recordSourceListener );
        }
      else
        {
        RecordSinkSupport.log.debug( this.getName( ) + " - New record source is null - no record source for this record sink" );
        this.setMetadata( null );
        }

      this.propertyChangeSupport.firePropertyChange( "recordSource", oldRecordSource, this.recordSource );
      }
    }

  public RecordMetadata getMetadata( )
    {
    return this.metadata;
    }

  public void setMetadata( RecordMetadata metadata )
    {
    RecordSinkSupport.log.debug( this.getName( ) + " - Setting new metadata" );
    RecordMetadata oldMetadata = this.metadata;
    this.metadata = metadata;
    this.propertyChangeSupport.firePropertyChange( "metadata", oldMetadata, this.metadata );
    if( RecordSinkSupport.log.isTraceEnabled( ) )
      RecordSinkSupport.log.trace( this.getName( ) + " - Metadata changed from " + oldMetadata + " to " + this.metadata );
    }
  }
