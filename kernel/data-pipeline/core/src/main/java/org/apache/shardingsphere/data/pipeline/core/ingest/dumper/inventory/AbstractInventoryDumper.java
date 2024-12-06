package org.apache.shardingsphere.data.pipeline.core.ingest.dumper.inventory;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.data.pipeline.core.channel.PipelineChannel;
import org.apache.shardingsphere.data.pipeline.core.exception.IngestException;
import org.apache.shardingsphere.data.pipeline.core.execute.AbstractPipelineLifecycleRunnable;
import org.apache.shardingsphere.data.pipeline.core.ingest.dumper.Dumper;
import org.apache.shardingsphere.data.pipeline.core.ingest.dumper.inventory.column.InventoryColumnValueReaderEngine;
import org.apache.shardingsphere.data.pipeline.core.ingest.dumper.inventory.position.InventoryDataRecordPositionCreator;
import org.apache.shardingsphere.data.pipeline.core.ingest.position.IngestPosition;
import org.apache.shardingsphere.data.pipeline.core.ingest.position.type.finished.IngestFinishedPosition;
import org.apache.shardingsphere.data.pipeline.core.metadata.loader.PipelineTableMetaDataLoader;
import org.apache.shardingsphere.data.pipeline.core.metadata.model.PipelineTableMetaData;
import org.apache.shardingsphere.data.pipeline.core.sqlbuilder.sql.PipelineInventoryDumpSQLBuilder;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class AbstractInventoryDumper extends AbstractPipelineLifecycleRunnable implements Dumper {
    
    protected final InventoryDumperContext dumperContext;
    
    protected final PipelineChannel channel;
    
    protected final DataSource dataSource;
    
    protected final PipelineTableMetaData tableMetaData;
    
    protected final InventoryDataRecordPositionCreator positionCreator;
    
    protected final PipelineInventoryDumpSQLBuilder sqlBuilder;
    
    protected final InventoryColumnValueReaderEngine columnValueReaderEngine;
    
    protected final AtomicReference<Statement> runningStatement = new AtomicReference<>();
    
    public AbstractInventoryDumper(final InventoryDumperContext dumperContext, final PipelineChannel channel, final DataSource dataSource, final PipelineTableMetaDataLoader metaDataLoader,
                                   final InventoryDataRecordPositionCreator positionCreator) {
        this.dumperContext = dumperContext;
        this.channel = channel;
        this.dataSource = dataSource;
        this.tableMetaData = getPipelineTableMetaData(metaDataLoader);
        this.positionCreator = positionCreator;
        DatabaseType databaseType = dumperContext.getCommonContext().getDataSourceConfig().getDatabaseType();
        sqlBuilder = new PipelineInventoryDumpSQLBuilder(databaseType);
        columnValueReaderEngine = new InventoryColumnValueReaderEngine(databaseType);
    }
    
    protected PipelineTableMetaData getPipelineTableMetaData(final PipelineTableMetaDataLoader metaDataLoader) {
        String schemaName = dumperContext.getCommonContext().getTableAndSchemaNameMapper().getSchemaName(dumperContext.getLogicTableName());
        String tableName = dumperContext.getActualTableName();
        return metaDataLoader.getTableMetaData(schemaName, tableName);
    }
    
    @Override
    protected void runBlocking() {
        IngestPosition position = dumperContext.getCommonContext().getPosition();
        if (position instanceof IngestFinishedPosition) {
            log.info("Ignored because of already finished.");
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            doRunBlocking(position, connection);
            // CHECKSTYLE:OFF
        } catch (final SQLException | RuntimeException ex) {
            // CHECKSTYLE:ON
            log.error("Inventory dump failed on {}", dumperContext.getActualTableName(), ex);
            throw new IngestException("Inventory dump failed on " + dumperContext.getActualTableName(), ex);
        }
    }
    
    protected abstract void doRunBlocking(IngestPosition position, Connection connection) throws SQLException;
}
