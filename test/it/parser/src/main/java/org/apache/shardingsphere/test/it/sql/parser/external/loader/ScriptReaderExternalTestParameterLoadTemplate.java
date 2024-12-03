/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.test.it.sql.parser.external.loader;

import org.apache.shardingsphere.test.loader.ExternalSQLTestParameter;
import org.apache.shardingsphere.test.loader.TestParameterLoadTemplate;
import org.h2.util.ScriptReader;

import java.io.StringReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * One case per file external test parameter load template.
 */
public final class ScriptReaderExternalTestParameterLoadTemplate implements TestParameterLoadTemplate {
    
    @Override
    public Collection<ExternalSQLTestParameter> load(final String sqlCaseFileName, final List<String> sqlCaseFileContent, final List<String> resultFileContent,
                                                     final String databaseType, final String reportType) {
        ScriptReader scriptReader = new ScriptReader(new StringReader(String.join("\n", sqlCaseFileContent)));
        Collection<ExternalSQLTestParameter> result = new LinkedList<>();
        while (true) {
            scriptReader.setSkipRemarks(true);
            String sql = scriptReader.readStatement();
            if (null == sql) {
                break;
            }
            result.add(new ExternalSQLTestParameter(sqlCaseFileName, databaseType, sql, reportType));
        }
        return result;
    }
    
}
