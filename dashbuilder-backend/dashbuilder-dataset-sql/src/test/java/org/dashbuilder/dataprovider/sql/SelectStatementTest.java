/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dashbuilder.dataprovider.sql;

import static org.dashbuilder.dataprovider.sql.SQLFactory.column;
import static org.dashbuilder.dataprovider.sql.SQLFactory.table;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.dashbuilder.dataprovider.sql.model.Select;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectStatementTest {

    @Mock
    Connection connection;
    
    @Mock
    DatabaseMetaData metadata;
    
    @Before
    public void setUp() throws Exception {
        when(connection.getMetaData()).thenReturn(metadata);
    }

    @Test
    public void testFixSQLCase() throws Exception {
        when(metadata.storesLowerCaseIdentifiers()).thenReturn(false);
        when(metadata.storesUpperCaseIdentifiers()).thenReturn(true);

        Select select = new Select(connection, JDBCUtils.H2);
        select.columns(column("id"));
        select.from(table("table"));
        
        assertEquals(select.getSQL(), "SELECT ID FROM TABLE");
    }

    @Test
    public void testFixSQLServerCase() throws Exception {
        when(metadata.storesLowerCaseIdentifiers()).thenReturn(false);
        when(metadata.storesUpperCaseIdentifiers()).thenReturn(true);

        Select select = new Select(connection, JDBCUtils.SQLSERVER);
        select.columns(column("r.*"));

        select.offset(1);
        select.limit(5);
        
        select.from("select r.*, mv.* FROM MappedVariable mv inner join ProcessInstanceLog pil on (pil.processInstanceId = mv.processInstanceId and pil.status = 3) inner join REQUEST r on (mv.variableId = r.requirement_id and mv.variableType = 'wissen.proyectoinversion.modelo.PedidoInversion') ");
        
        System.out.println(select.getSQL());
        
        assertEquals(select.getSQL(), "WITH result_set AS ( SELECT ROW_NUMBER() OVER (ORDER BY REQUIREMENT_ID ASC ) AS [row_number],  REQUIREMENT_ID, COMPANYID, REQUIREMENT_STATE, MAP_VAR_ID FROM (SELECT R.*, MV.* FROM MAPPEDVARIABLE MV INNER JOIN PROCESSINSTANCELOG PIL ON (PIL.PROCESSINSTANCEID = MV.PROCESSINSTANCEID AND PIL.STATUS = 3) INNER JOIN REQUEST R ON (MV.VARIABLEID = R.REQUIREMENT_ID AND MV.VARIABLETYPE = 'wissen.proyectoinversion.modelo.PedidoInversion') ) \"dbSQL\")  SELECT * FROM result_set WHERE [row_number] BETWEEN 1 AND 5");
    }
    
    @Test
    public void testCountSQLServer() throws Exception {
        when(metadata.storesLowerCaseIdentifiers()).thenReturn(false);
        when(metadata.storesUpperCaseIdentifiers()).thenReturn(true);

        Select select = new Select(connection, JDBCUtils.SQLSERVER);
        select.columns(column("r.requirement_id"));

        select.offset(0);
        select.limit(0);
        
        select.from("select r.*, mv.* FROM MappedVariable mv inner join ProcessInstanceLog pil on (pil.processInstanceId = mv.processInstanceId and pil.status = 3) inner join REQUEST r on (mv.variableId = r.requirement_id and mv.variableType = 'wissen.proyectoinversion.modelo.PedidoInversion') ");
        
        String countQuerySQL = JDBCUtils.SQLSERVER.getCountQuerySQL(select);
        
        System.out.println(countQuerySQL);
        
        System.out.println(select.getSQL());
        
        assertEquals(select.getSQL(), "WITH result_set AS ( SELECT ROW_NUMBER() OVER (ORDER BY REQUIREMENT_ID ASC ) AS [row_number],  REQUIREMENT_ID, COMPANYID, REQUIREMENT_STATE, MAP_VAR_ID FROM (SELECT R.*, MV.* FROM MAPPEDVARIABLE MV INNER JOIN PROCESSINSTANCELOG PIL ON (PIL.PROCESSINSTANCEID = MV.PROCESSINSTANCEID AND PIL.STATUS = 3) INNER JOIN REQUEST R ON (MV.VARIABLEID = R.REQUIREMENT_ID AND MV.VARIABLETYPE = 'wissen.proyectoinversion.modelo.PedidoInversion') ) \"dbSQL\")  SELECT * FROM result_set WHERE [row_number] BETWEEN 1 AND 5");
    }
    
    @Test
    public void testSQLServerOrderBy() throws Exception {
        when(metadata.storesLowerCaseIdentifiers()).thenReturn(false);
        when(metadata.storesUpperCaseIdentifiers()).thenReturn(true);

        Select select = new Select(connection, JDBCUtils.SQLSERVER);
        select.columns(column("requirement_id"));
        select.columns(column("companyId"));
        select.columns(column("requirement_state"));
        select.columns(column("MAP_VAR_ID"));
        select.orderBy(column("companyId").asc());
        select.offset(3);
        select.limit(5);
        
        select.from("select r.*, mv.* FROM MappedVariable mv inner join ProcessInstanceLog pil on (pil.processInstanceId = mv.processInstanceId and pil.status = 3) inner join REQUEST r on (mv.variableId = r.requirement_id and mv.variableType = 'wissen.proyectoinversion.modelo.PedidoInversion') ");
        
        assertEquals(select.getSQL(), "WITH result_set AS ( SELECT ROW_NUMBER() OVER (ORDER BY COMPANYID ASC ) AS [row_number],  REQUIREMENT_ID, COMPANYID, REQUIREMENT_STATE, MAP_VAR_ID FROM (SELECT R.*, MV.* FROM MAPPEDVARIABLE MV INNER JOIN PROCESSINSTANCELOG PIL ON (PIL.PROCESSINSTANCEID = MV.PROCESSINSTANCEID AND PIL.STATUS = 3) INNER JOIN REQUEST R ON (MV.VARIABLEID = R.REQUIREMENT_ID AND MV.VARIABLETYPE = 'wissen.proyectoinversion.modelo.PedidoInversion') ) \"dbSQL\")  SELECT * FROM result_set WHERE [row_number] BETWEEN 3 AND 5");
    }
    
    @Test
    public void testKeepColumnAsIs() throws Exception {
        when(metadata.storesLowerCaseIdentifiers()).thenReturn(false);
        when(metadata.storesUpperCaseIdentifiers()).thenReturn(true);

        Select select = new Select(connection, JDBCUtils.H2);
        select.columns(column("id"));
        select.from("SELECT ID as \"id\" FROM TABLE");

        assertEquals(select.getSQL(), "SELECT \"id\" FROM (SELECT ID AS \"id\" FROM TABLE) \"dbSQL\"");
    }
}
