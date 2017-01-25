#!/usr/bin/env php
<?php

exit (main());

/**
 * Application main function.
 *
 * @return int
 */
function main()
{
    $opt = getopt("a:A:d:eh:su:p:o:v:m:t:?");
    if (isset($opt['?']) || !isset($opt['d'])) {
        printHelp();
        return -1;
    }

    $config = array();
    // Connection settings
    $config['db'] = getOption($opt, 'd');
    $config['host'] = getOption($opt, 'h', 'localhost');
    $config['user'] = getOption($opt, 'u', 'root');
    $config['pass'] = getOption($opt, 'p', '');

    // Audit settings
    $config['tables'] = getOption($opt, 't', null);
    $config['exclude'] = getOption($opt, 'e', null) !== null ? true : false;
    $config['separate'] = getOption($opt, 's', null) !== null ? true : false;
    $config['prefix'] = getOption($opt, 'a', null);
    $config['suffix'] = getOption($opt, 'A', '_audit');

    // Script settings
    $config['audit_dir'] = getOption($opt, 'm', './cdc_audit_gen');
    $config['verbosity'] = getOption($opt, 'v', 4);
    $config['stdout'] = STDOUT;

    if (isset($opt['o'])) {
        if (!$fh = fopen($opt['o'], 'w')) {
            die("Could not open {$opt['o']} for writing");
        }
        $config['stdout'] = $fh;
    }

    $engine = new CdcAuditGenMysql($config);
    $success = $engine->run();

    fclose($config['stdout']);
    return $success ? 0 : -1;
}


/**
 * Get CLI arguments.
 *
 * @param array $opt getopts array.
 * @param string $key Key to look for in array.
 * @param string $default Value to return if key not set.
 * @return mixed
 */
function getOption($opt, $key, $default=null)
{
    return isset($opt[$key]) ? $opt[$key] : $default;
}


/**
 * Print help text.
 *
 * @return void
 */
function printHelp()
{
    echo
        "Usage: cdc_audit_gen_mysql.php [Options] -d <db> [-h <host> -d <db> -u <user> -p <pass>]\n" .
        "\n" .
        "   Required:\n" .
        "   -d DB              database name\n" .
        "\n" .
        "   Options:\n" .
        "   -h HOST            address of machine running mysql.          default = localhost\n" .
        "   -u USER            mysql username.                            default = root\n" .
        "   -p PASS            mysql password.\n" .
        "   -m DIR             path to write audit files.                 default = ./cdc_audit_gen\n" .
        "   -t TABLES          comma separated list of tables to audit.   default = generate for all tables\n" .
        "   -e                 invert -t, exclude the listed tables.\n" .
        "   -s                 separate triggers, do not rebuild and drop\n" .
        "                      existing triggers (trigger name will be\n" .
        "                      <table>_audit_<event>).\n" .
        "   -A SUFFIX          suffix for audit tables.                   default = '_audit'\n" .
        "   -a PREFIX          prefix for audit tables, replaces suffix.\n" .
        "   -o FILE            send all output to FILE                    default = send output to STDOUT.\n" .
        "   -v <INT>           verbosity level.  default = 4\n" .
        "                        3 = silent except fatal error.\n" .
        "                        4 = silent except warnings.\n" .
        "                        6 = informational.\n" .
        "                        7 = debug.\n" .
        "   -?                 print this help message.\n";
}


/**
 * Generator for SQL audit tables and triggers.
 */
class CdcAuditGenMysql
{
    private $host;
    private $user;
    private $pass;
    private $db;

    private $tables;
    private $exclude;
    private $separate;
    private $prefix;
    private $suffix;

    private $output_dir;
    private $verbosity;
    private $stdout;

    private $connection;

    /**
     * Constructor.
     *
     * @param array $config Config settings in associative array form.
     */
    public function __construct($config)
    {
        $this->db = $config['db'];
        $this->host = $config['host'];
        $this->user = $config['user'];
        $this->pass = $config['pass'];

        if (!empty($config['tables'])) {
            $this->tables = array();
            foreach (explode(',', $config['tables']) as $table) {
               $this->tables[trim($table)] = true;
            }
        }
        $this->exclude = $config['exclude'];
        $this->separate = $config['separate'];
        $this->prefix = $config['prefix'];
        $this->suffix = $config['suffix'];

        $this->output_dir = $config['audit_dir'];
        $this->verbosity = $config['verbosity'];
        $this->stdout = $config['stdout'];
    }

    /**
     * Log a message (or not) depending on log level.
     *
     * @param string $message Log message.
     * @param int $level Log level.
     * @return void
     */
    private function log($message, $level)
    {
        if ($level <= $this->verbosity) {
            fprintf($this->stdout, "%s\n", $message);
        }
    }

    /**
     * Get audit table name.
     *
     * @param string $table
     * @return string
     */
    private function getAuditTableName($table)
    {
        if ($this->prefix) {
            $nameMask = '%1$s%2$s';
            $affix = $this->prefix;
        } else {
            $nameMask = '%2$s%1$s';
            $affix = $this->suffix;
        }
        return sprintf($nameMask, $affix, $table);
    }

    /**
     * Get filename for SQL audit file.
     *
     * @param string $table
     * @return string
     */
    private function getAuditFilename($table)
    {
        return sprintf("%s.audit.sql", $table);
    }

    /**
     * Execute the engine.
     *
     * @return bool
     */
    public function run()
    {
        return $this->createDbAudit();
    }

    /**
     * Query INFORMATION_SCHEMA and generate SQL for audit tables and triggers.
     *
     * @return bool
     */
    private function createDbAudit()
    {
        try {
            /**
             * Create path if not already exists
             */
            $this->log("Checking if path exists: {$this->output_dir}", LOG_DEBUG);
            if (!is_dir($this->output_dir)) {
                $this->log("Path does not exist.  creating: {$this->output_dir}", LOG_DEBUG);
                if (!@mkdir($this->output_dir)) {
                    throw new Exception("Cannot mkdir {$this->output_dir}");
                }
                $this->log("Path created: {$this->output_dir}", LOG_INFO);
            }

            /**
             * Delete audit file if already exists
             */
            $this->log("Deleting audit table definition files in {$this->output_dir}",  LOG_DEBUG);
            foreach (glob($this->output_dir . '/*.audit.sql') as $file) {
                if (is_array($this->tables)) {
                    $tableName = explode('.', $file)[0];
                    if ((!$this->exclude && !isset($this->tables[$tableName]))
                            || ($this->exclude && isset($this->tables[$tableName]))) {
                        continue;
                    }
                }
                if (!@unlink($file)) {
                    throw new Exception("Cannot unlink old file " . $file);
                }
                $this->log("Deleted $file", LOG_DEBUG);
            }
            $this->log("Deleted audit table definition files in {$this->output_dir}", LOG_INFO);

            /**
             * Connect to the MySQL server
             */
            $this->log("Connecting to mysql. host={$this->host}, user={$this->user}, pass={$this->pass}", LOG_DEBUG);
            $dsn = "mysql:host={$this->host};dbname={$this->db}";
            $opt = [
                PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES   => false,
            ];
            $this->connection = new PDO($dsn, $this->user, $this->pass, $opt);

            $this->log('Connected to mysql. Getting tables.',  LOG_INFO);

            /**
             * Get all tables
             */
            $stmt = $this->connection->prepare("SHOW TABLES");
            $stmt->execute();
            while($table = $stmt->fetch()["Tables_in_{$this->db}"]) {
                if (is_array($this->tables)) {
                    if ((!$this->exclude && !isset($this->tables[$table]))
                            || ($this->exclude && isset($this->tables[$table]))) {
                        $this->log("Found table $table.  Not in output list.  skipping", LOG_INFO);
                        continue;
                    }
                }

                /**
                 * Ignore audit tables
                 */
                if (stristr($table, $this->prefix) || stristr($table, $this->suffix)) {
                    $this->log("Found table $table.  Appears to be an audit table.  skipping", LOG_INFO);
                    continue;
                }

                /**
                 * Get table info
                 */
                $tableStmt = $this->connection->prepare(
                    'SELECT COLUMN_NAME AS Field, ' .
                    '       COLUMN_TYPE AS Type, ' .
                    '       IS_NULLABLE AS `Null`, ' .
                    '       COLUMN_KEY AS `Key`, ' .
                    '       COLUMN_DEFAULT AS `Default`, ' .
                    '       COLUMN_COMMENT AS Comment ' .
                    'FROM   INFORMATION_SCHEMA.COLUMNS ' .
                    'WHERE  TABLE_SCHEMA = ? ' .
                    '       AND TABLE_NAME = ?'
                );
                $tableStmt->execute([$this->db, $table]);
                $info = $tableStmt->fetchAll();

                /**
                 * Get triggers associated with table
                 */
                $triggerStmt = $this->connection->prepare(
                    'SELECT TRIGGER_NAME AS `TriggerName`, ' .
                    '       ACTION_STATEMENT AS `TriggerAction`, ' .
                    '       EVENT_MANIPULATION AS `TriggerEvent` ' .
                    'FROM   INFORMATION_SCHEMA.TRIGGERS ' .
                    'WHERE  EVENT_OBJECT_TABLE = ? ' .
                    '       AND ACTION_TIMING = "AFTER" '.
                    '       AND TRIGGER_SCHEMA = ?'
                );
                $triggerStmt->execute([$table, $this->db]);
                $triggers = $triggerStmt->fetchAll();

                /**
                 * Generate audit table and triggers
                 */
                $this->log("Processing table $table", LOG_DEBUG);
                $this->writeAuditTable($table, $info);
                $this->writeAuditTriggers($table, $info, $triggers);
            }
            $this->log("Successfully generated audit tables and triggers in {$this->output_dir}", LOG_INFO);
        } catch(Exception $e) {
            $this->log($e->getMessage() . ' -- line: ' . $e->getLine(), LOG_ERR);
            return false;
        }
        return true;
    }

    /**
     * Generate audit table SQL for $table and write/execute SQL.
     *
     * @param string $table Table name.
     * @param array $info Table structure information.
     * @return void
     */
    private function writeAuditTable($table, $info)
    {
        $headerMask =
            '/**' . "\n" .
            ' * Audit table for table `%1$s`.' . "\n" .
            ' *' . "\n" .
            ' * !!! DO NOT MODIFY THIS FILE MANUALLY !!!' . "\n" .
            ' *' . "\n" .
            ' * This file is auto-generated and is NOT intended' . "\n" .
            ' * for manual modifications/extensions.' . "\n" .
            ' *' . "\n" .
            ' * For additional documentation, see:' . "\n" .
            ' * https://github.com/dan-da/cdc_audit' . "\n" .
            ' *' . "\n" .
            ' */' . "\n";
        $output = sprintf($headerMask, $table);

        // Index definition mask
        $indexMask = 'INDEX (%1$s)';

        // Table definition mask
        $tableMask =
            'CREATE TABLE IF NOT EXISTS `%1$s` (' . "\n" .
            '%2$s' . "\n" .
            ');' . "\n";

        // Column definition mask
        $columnMask = '`%1$s` %2$s %3$s %4$s %5$s COMMENT \'%6$s\'';

        // Prepend audit fields to list of table fields
        array_unshift($info, array('Field' => 'audit_user', 'Type' => 'VARCHAR(255)', 'Null' => false, 'Comment' => 'User triggering source table event'));
        array_unshift($info, array('Field' => 'audit_event', 'Type' => 'CHAR(6)', 'Null' => false, 'Comment' => 'Type of source table event'));
        array_unshift($info, array('Field' => 'audit_timestamp', 'Type' => 'TIMESTAMP', 'Null' => false, 'Comment' => 'Timestamp of source table event'));
        array_unshift($info, array('Field' => 'audit_pk', 'Type' => 'INT(11) UNSIGNED', 'Null' => false, 'Comment' => 'Audit table primary key, useful for sorting since MySQL time data types are only granular to second level.'));

        $pkfields = array();
        foreach ($info as $column) {
            $comment = @$column['Comment'];
            if (@$column['Key'] == 'PRI') {
               $pkfields[] = sprintf('`%s`', $column['Field']);
               $comment = 'Primary key in source table ' . $table;
            }

            $lines[] = sprintf(
                $columnMask,
                $column['Field'],
                strtoupper($column['Type']),
                $column['Null'] === 'YES' ? 'NULL' : 'NOT NULL',
                $column['Field'] === 'audit_pk' ? 'PRIMARY KEY' : '',
                $column['Field'] === 'audit_pk' ? 'AUTO_INCREMENT' : '',
                str_replace("'", "''", $comment)
            );
        }

        if (count($pkfields)) {
            $lines[] = sprintf($indexMask, implode(', ', $pkfields));
        }
        $lines[] = sprintf($indexMask, '`audit_timestamp`');

        $output .= sprintf($tableMask, $this->getAuditTableName($table), implode(",\n", $lines)) . "\n\n";

        /**
         * Write to file
         */
        $filename = $this->getAuditFilename($table);
        $pathname = $this->output_dir . '/' . $filename;
        $this->log("Writing table to $pathname", LOG_INFO);
        if (!@file_put_contents($pathname, $output)) {
            throw new Exception("Error writing file $pathname");
        }
    }

    /**
     * Generate audit triggers SQL for table and write/execute SQL.
     *
     * @param string $table Table name.
     * @param array $info Table structure information.
     * @param array $triggers Existing trigger information.
     * @return void
     */
    private function writeAuditTriggers($table, $info, $triggers)
    {
        $headerMask =
            '/**' . "\n" .
            ' * Audit triggers for table `%1$s`.' . "\n" .
            ' *' . "\n" .
            ' * For additional documentation, see:' . "\n" .
            ' * https://github.com/dan-da/cdc_audit' . "\n" .
            ' *' . "\n" .
            ' */' . "\n";

        $output = sprintf($headerMask, $table);

        $dropTriggerMask = 'DROP TRIGGER IF EXISTS `%1$s`;' . "\n";

        $triggersMask =
            '-- %1$s AFTER INSERT trigger.' . "\n" .
            'DELIMITER @@' . "\n" .
            'CREATE TRIGGER `%5$s` AFTER INSERT ON `%1$s`' . "\n" .
            ' FOR EACH ROW BEGIN' . "\n" .
            '  INSERT INTO `%2$s` (%3$s) VALUES(%4$s);' . "\n" .
            '  %6$s;' . "\n" .
            ' END;' . "\n" .
            '@@' . "\n" .
            '-- %1$s AFTER UPDATE trigger.' . "\n" .
            'DELIMITER @@' . "\n" .
            'CREATE TRIGGER `%8$s` AFTER UPDATE ON `%1$s`' . "\n" .
            ' FOR EACH ROW BEGIN' . "\n" .
            '  INSERT INTO `%2$s` (%3$s) VALUES(%7$s);' . "\n" .
            '  %9$s;' . "\n" .
            ' END;' . "\n" .
            '@@' . "\n" .
            '-- %1$s AFTER DELETE trigger.' . "\n" .
            'DELIMITER @@' . "\n" .
            'CREATE TRIGGER `%11$s` AFTER DELETE ON `%1$s`' . "\n" .
            ' FOR EACH ROW BEGIN' . "\n" .
            '  INSERT INTO `%2$s` (%3$s) VALUES(%10$s);' . "\n" .
            '  %12$s;' . "\n" .
            ' END;' . "\n" .
            '@@' . "\n";

        $auditTable = $this->getAuditTableName($table);

        $oldTriggers = array(
            'insert' => null,
            'update' => null,
            'delete' => null,
        );

        /**
         * Drop existing AFTER triggers for this table.
         */
        foreach ($triggers as $trigger) {
            if ($this->separate && !stristr($trigger['TriggerName'], '_audit_')) {
                $this->log("Non-audit trigger encountered: {$trigger['TriggerName']}.  skipping", LOG_INFO);
                continue;
            } elseif(!$this->separate) {
                /**
                 * Extract and reuse trigger action
                 */
                if (strtolower(substr($trigger['TriggerAction'], 0, 5)) !== 'begin'
                        || strtolower(substr($trigger['TriggerAction'], - 3)) !== 'end') {
                    /**
                     * Trigger action is broken
                     */
                    $this->log("Action statement didn't begin and/or end correctly in {$trigger['TriggerName']}.  skipping", LOG_ERR);
                    continue;
                }

                /**
                 * Remove any audit statement; any line containing $auditTable
                 */
                $actionLines = array();
                foreach (explode("\n", substr($trigger['TriggerAction'], 5, - 3)) as $line) {
                    if (!strstr($line, $auditTable)) {
                        $actionLines[] = $line;
                    }
                }

                $oldTriggers[strtolower($trigger['TriggerEvent'])] = array(
                    'Name' => $trigger['TriggerName'],
                    'Action' => implode("\n", $actionLines),
                );
                $this->log("Extracted action from {$trigger['TriggerName']}", LOG_INFO);
            }
            $output .= sprintf($dropTriggerMask, $trigger['TriggerName']);
        }

        $fields = array();
        $newValues = array();
        $oldValues = array();
        foreach ($info as $column) {
            $fields[] = $column['Field'];
            $newValues[] = "NEW.`{$column['Field']}`";
            $oldValues[] = "OLD.`{$column['Field']}`";
        }

        $insertValues = $newValues;
        $updateValues = $newValues;
        $deleteValues = $oldValues;

        // Add 'audit_timestamp' field and value
        array_unshift($fields, 'audit_timestamp');
        array_unshift($insertValues, 'CURRENT_TIMESTAMP');
        array_unshift($updateValues, 'CURRENT_TIMESTAMP');
        array_unshift($deleteValues, 'CURRENT_TIMESTAMP');

        // Add 'audit_event' field and value
        array_unshift($fields, 'audit_event');
        array_unshift($insertValues, "'insert'");
        array_unshift($updateValues, "'update'");
        array_unshift($deleteValues, "'delete'");

        // Add 'audit_user' field and value
        array_unshift($fields, 'audit_user');
        array_unshift($insertValues, 'USER()');
        array_unshift($updateValues, 'USER()');
        array_unshift($deleteValues, 'USER()');

        foreach ($fields as &$field) {
            $field = sprintf('`%s`', $field);
        }

        $colnames = implode(', ', $fields);
        $insertValues = implode(', ', $insertValues);
        $updateValues = implode(', ', $updateValues);
        $deleteValues = implode(', ', $deleteValues);

        $output .= sprintf(
            $triggersMask,
            $table,
            $auditTable,
            $colnames,
            $insertValues,
            $this->separate ? "{$table}_audit_insert" : "${table}_after_insert",
            !empty($oldTriggers['insert']) ? $oldTriggers['insert']['Action'] : '',
            $updateValues,
            $updateName = $this->separate ? "{$table}_audit_update" : "${table}_after_update",
            !empty($oldTriggers['update']) ? $oldTriggers['update']['Action'] : '',
            $deleteValues,
            $deleteName = $this->separate ? "{$table}_audit_delete" : "${table}_after_delete",
            !empty($oldTriggers['delete']) ? $oldTriggers['delete']['Action'] : ''
        );

        /**
         * Write to file
         */
        $filename = $this->getAuditFilename($table);
        $pathname = $this->output_dir . '/' . $filename;
        $this->log("Writing triggers to $pathname", LOG_INFO);
        if (!@file_put_contents($pathname, $output, FILE_APPEND)) {
            throw new Exception("Error writing file $pathname");
        }
    }
}
