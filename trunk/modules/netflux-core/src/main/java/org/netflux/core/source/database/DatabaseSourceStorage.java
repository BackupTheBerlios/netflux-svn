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
package org.netflux.core.source.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.netflux.core.FieldMetadata;
import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.source.SourceDataStorage;
import org.netflux.core.source.SourceDataStorageException;

/**
 * @author jgonzalez
 */
public class DatabaseSourceStorage implements SourceDataStorage
  {
  private DataSource        dataSource;
  private String            sqlStatement;
  private RecordMetadata    recordMetadata;
  private Connection        connection;
  private PreparedStatement preparedStatement;
  private ResultSet         resultSet;

  /**
   * @return Returns the dataSource.
   */
  public DataSource getDataSource( )
    {
    return this.dataSource;
    }

  /**
   * @param dataSource The dataSource to set.
   */
  public void setDataSource( DataSource dataSource )
    {
    this.dataSource = dataSource;
    this.updateMetadata( );
    }

  /**
   * @return Returns the sqlStatement.
   */
  public String getSqlStatement( )
    {
    return this.sqlStatement;
    }

  /**
   * @param sqlStatement The sqlStatement to set.
   */
  public void setSqlStatement( String sqlStatement )
    {
    this.sqlStatement = sqlStatement;
    this.updateMetadata( );
    }

  protected void updateMetadata( )
    {
    if( this.getDataSource( ) != null && this.getSqlStatement( ) != null )
      {
      // TODO: Exception handling
      Connection connection = null;
      PreparedStatement preparedStatement = null;
      try
        {
        connection = this.getDataSource( ).getConnection( );
        preparedStatement = connection.prepareStatement( this.getSqlStatement( ) );
        ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData( );

        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>( resultSetMetaData.getColumnCount( ) );
        for( int columnIndex = 1; columnIndex <= resultSetMetaData.getColumnCount( ); columnIndex++ )
          {
          FieldMetadata currentFieldMetadata = new FieldMetadata( resultSetMetaData.getColumnName( columnIndex ), resultSetMetaData
              .getColumnType( columnIndex ) );
          currentFieldMetadata.setNullable( resultSetMetaData.isNullable( columnIndex ) == ResultSetMetaData.columnNullable );
          currentFieldMetadata.setScale( resultSetMetaData.getScale( columnIndex ) );
          currentFieldMetadata.setPrecision( resultSetMetaData.getPrecision( columnIndex ) );

          fieldMetadata.add( currentFieldMetadata );
          }

        this.recordMetadata = new RecordMetadata( fieldMetadata );
        }
      catch( SQLException e )
        {
        // TODO: Exception handling
        this.recordMetadata = null;
        }
      finally
        {
        if( preparedStatement != null )
          {
          try
            {
            preparedStatement.close( );
            }
          catch( SQLException e )
            {}
          }
        if( connection != null )
          {
          try
            {
            connection.close( );
            }
          catch( SQLException e )
            {}
          }
        }
      }
    else
      {
      this.recordMetadata = null;
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.source.SourceDataStorage#getMetadata()
   */
  public RecordMetadata getMetadata( )
    {
    return this.recordMetadata;
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.source.SourceDataStorage#nextRecord()
   */
  public Record nextRecord( ) throws SourceDataStorageException
    {
    try
      {
      if( this.connection == null )
        {
        this.connection = this.getDataSource( ).getConnection( );
        this.preparedStatement = this.connection.prepareStatement( this.getSqlStatement( ) );
        this.resultSet = this.preparedStatement.executeQuery( );
        }

      if( this.resultSet.next( ) )
        {
        Record record = new Record( this.recordMetadata );
        for( int columnIndex = 1; columnIndex <= this.recordMetadata.getFieldCount( ); columnIndex++ )
          {
          this.populateField( record, columnIndex );
          }
        return record;
        }
      else
        {
        return Record.END_OF_DATA;
        }
      }
    catch( SQLException exc )
      {
      throw new SourceDataStorageException( "SQL exception while trying to retrieve record from database.", null, null, true, exc );
      }
    }

  /**
   * @param line
   * @param record
   * @param fieldScanner
   * @param fieldFormat
   * @throws SourceDataStorageException
   */
  protected void populateField( Record record, int columnIndex ) throws SourceDataStorageException
    {
    FieldMetadata fieldMetadata = this.recordMetadata.getFieldMetadata( ).get( columnIndex - 1 );
    try
      {
      switch( fieldMetadata.getType( ) )
        {
        case Types.CHAR:
        case Types.VARCHAR:
          record.setValue( fieldMetadata.getName( ), this.resultSet.getString( columnIndex ) );
          break;

        case Types.DATE:
        case Types.TIMESTAMP:
          record.setValue( fieldMetadata.getName( ), this.resultSet.getDate( columnIndex ) );
          break;

        case Types.SMALLINT:
        case Types.INTEGER:
          // TODO: Check if the int type is able to handle SQL INTEGER type
          int intValue = this.resultSet.getInt( columnIndex );
          record.setValue( fieldMetadata.getName( ), (!this.resultSet.wasNull( )) ? new Integer( intValue ) : null );
          break;

        case Types.BIGINT:
          // TODO: Should I take into account precision and scale?
          record.setValue( fieldMetadata.getName( ), this.resultSet.getBigDecimal( columnIndex ) );
          break;

        case Types.DECIMAL:
          record.setValue( fieldMetadata.getName( ), this.resultSet.getBigDecimal( columnIndex ) );
          break;

        case Types.FLOAT:
          float floatValue = this.resultSet.getFloat( columnIndex );
          record.setValue( fieldMetadata.getName( ), (!this.resultSet.wasNull( )) ? new Float( floatValue ) : null );
          break;

        case Types.DOUBLE:
          double doubleValue = this.resultSet.getDouble( columnIndex );
          record.setValue( fieldMetadata.getName( ), (!this.resultSet.wasNull( )) ? new Double( doubleValue ) : null );
          break;

        case Types.BOOLEAN:
          boolean booleanValue = this.resultSet.getBoolean( columnIndex );
          record.setValue( fieldMetadata.getName( ), (!this.resultSet.wasNull( )) ? new Boolean( booleanValue ) : null );
          break;

        default:
          throw new SourceDataStorageException( "Type not supported: " + fieldMetadata.getType( ) + " while trying to assign: "
              + fieldMetadata.getName( ), record, null );
        }
      }
    catch( SQLException exc )
      {
      throw new SourceDataStorageException( "SQL exception while trying to assign: " + fieldMetadata.getName( ), record, null, exc );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.source.SourceDataStorage#close()
   */
  public void close( )
    {
    try
      {
      this.resultSet.close( );
      }
    catch( SQLException e )
      {}
    try
      {
      this.preparedStatement.close( );
      }
    catch( SQLException e )
      {}
    try
      {
      this.connection.close( );
      }
    catch( SQLException e )
      {}
    }
  }
