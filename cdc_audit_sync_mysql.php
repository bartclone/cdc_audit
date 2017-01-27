#!/usr/bin/env php
<?php

// TODO: add feature to wipe only older than a specific age.

exit (main());

/**
 * Application main function.
 *
 * @return int
 */
function main()
{
    $opt = getopt("a:A:d:eh:u:p:o:v:wm:t:?");
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
    $config['wipe'] = getOption($opt, 'w', null) !== null ? true : false;
    $config['prefix'] = getOption($opt, 'a', null);
    $config['suffix'] = getOption($opt, 'A', '_audit');

    // Script settings
    $config['output_dir'] = getOption($opt, 'm', './cdc_audit_sync');
    $config['verbosity'] = getOption($opt, 'v', 4);
    $config['stdout'] = STDOUT;

    if (isset($opt['o'])) {
        if (!$fh = @fopen($opt['o'], 'w')) {
            die("Could not open {$opt['o']} for writing");
        }
        $config['stdout'] = $fh;
    }

    $engine = new CdcAuditSyncMysql($config);
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
        "Usage: cdc_audit_sync_mysql.php [Options] -d <db> [-h <host> -d <db> -u <user> -p <pass>]\n" .
        "\n" .
        "   Required:\n" .
        "   -d DB              database name\n" .
        "\n" .
        "   Options:\n" .
        "   -h HOST            address of machine running mysql.          default = localhost\n" .
        "   -u USER            mysql username.                            default = root\n" .
        "   -p PASS            mysql password.\n" .
        "   -m DIR             path to write audit files.                 default = ./cdc_audit_sync\n" .
        "   -t TABLES          comma separated list of tables to audit.   default = generate for all tables\n" .
        "   -e                 invert -t, exclude the listed tables.\n" .
        "   -w                 wipe all but the very last audit row after\n" .
        "                      syncing through truncate and a tmp table.\n" .
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
 * Sync audit table rows to CSV files.
 */
class CdcAuditSyncMysql
{
    private $host;
    private $user;
    private $pass;
    private $db;

    private $tables;
    private $exclude;
    private $wipe;
    private $prefix;
    private $suffix;

    private $output_dir;
    private $verbosity;
    private $stdout;

    private $connection;
    private $csvFile;

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
        $this->prefix = $config['prefix'];
        $this->suffix = $config['suffix'];

        $this->output_dir = $config['output_dir'];
        $this->wipe = $config['wipe'];
        $this->verbosity = $config['verbosity'];
        $this->stdout = $config['stdout'];
    }

    /**
     * Execute the engine.
     *
     * @return bool
     */
    public function run()
    {
        return $this->syncAuditTables();;
    }

    /**
     * Query INFORMATION_SCHEMA and sync audit tables to CSV files.
     *
     * @return bool
     */
    private function syncAuditTables()
    {
        try {
            // Create path if not already exists
            $this->ensureDirExists($this->output_dir);

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
                 * Ignore non-audit tables
                 */
                if (!stristr($table, $this->prefix) && !stristr($table, $this->suffix)) {
                    $this->log("Found table $table.  Appears to be a non-audit table.  skipping", LOG_INFO);
                    continue;
                }

                $this->syncTable($table);
            }
            $this->log("Successfully synced audit tables to {$this->output_dir}", LOG_WARNING);
        } catch(Exception $e) {
            $this->log($e->getMessage() . ' -- line: ' . $e->getLine(), LOG_ERR);
            return false;
        }
        return true;
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
     * Ensure that directory exists.
     *
     * @throws Exception if directory can't be created.
     * @param string $path Path to directory.
     * @return void
     */
    private function ensureDirExists($path)
    {
        $this->log("Checking if path exists: {$this->output_dir}", LOG_DEBUG);
        if (!is_dir($this->output_dir)) {
            $this->log("Path does not exist.  creating: {$this->output_dir}", LOG_DEBUG);
            if (!@mkdir($this->output_dir)) {
                throw new Exception("Cannot mkdir {$this->output_dir}");
            }
            $this->log("Path created: {$this->output_dir}", LOG_INFO);
        }
    }

    /**
     * Sync audit table to CSV file.
     *
     * @throws Exception if file cannot be opened for writing.
     * @param string $table Table name.
     * @return void
     */
    private function syncTable($table)
    {
        $this->log("Processing table $table", LOG_INFO);

        $pkLast = $this->getLastCsvRowPk($table);
        $stmt = $this->connection->prepare("SELECT * FROM $table WHERE audit_pk > ?");
        $stmt->execute([$pkLast]);

        $mode = $pkLast === -1 ? 'w' : 'a';
        $file = fopen($this->csvPath($table), $mode);

        if (!$file) {
            throw new Exception("Unable to open file {$this->csv_path($table)} for writing");
        }

        if ($pkLast === -1) {
            $this->writeCsvHeaderRow($file, $stmt);
        }

        while ($row = $stmt->fetch(PDO::FETCH_NUM)) {
            fputcsv($file, $row);
        }

        fclose($file);

        if ($this->wipe) {
            $this->wipeAuditTable($table);
        }
    }

    /**
     * Delete all but the last row from audit table.
     *
     * @throws Exception if SQL delete query fails.
     * @param string $table Table name.
     * @return void
     */
    private function wipeAuditTable($table)
    {
        $this->log("wiping audit table: $table", LOG_INFO);

        /**
         * Delete is slow but plays well with concurrent connections.
         * An incremental delete is used to avoid hitting the DB too hard
         * when wiping large tables.
         */
        $increment = 100;

        while (true) {
            $stmt = $this->connection->prepare("SELECT Count(audit_pk) AS count, Min(audit_pk) AS min, max(audit_pk) AS max FROM `$table`");
            $stmt->execute();
            $row = $stmt->fetch();

            $count = $row['count'];
            $min = $row['min'];
            $max = $row['max'];

            if ($count <= 1 || !$max) {
                break;
            }

            $deleteMax = min($min + $increment, $max);
            // TODO: Fix log message, numbers not always correct
            $this->log("wiping audit table rows $min to $deleteMax", LOG_INFO);

            $stmt = $this->connection->prepare("DELETE FROM `$table` WHERE audit_pk >= ? AND audit_pk < ?");
            if (!$stmt->execute([$min, $deleteMax])) {
                throw new Exception("mysql error while wiping " . $deleteMax - $min . " rows.");
            }
            sleep(1);
        }
    }

    /**
     * Write header row with column names to table CSV file.
     *
     * @param string $filename Path to file.
     * @oaram PDOStatement $result Result set from corresponding table.
     * @return void
     */
    private function writeCsvHeaderRow($filename, $result)
    {
        $cols = array();

        for ($i = 0; $i < $result->columnCount(); $i++) {
            $meta = $result->getColumnMeta($i);
            $cols[] = $meta['name'];
        }

        fputcsv($filename, $cols);
    }

    /**
     * Get primary key value of last row in table CSV file.
     *
     * @param string $table Table name.
     * @return mixed
     */
    private function getLastCsvRowPk($table)
    {
        $lastPk = -1;
        $row = str_getcsv($this->getLastLine($this->csvPath($table)));
        $count = count($row);

        if ($count >= 5 && is_numeric($row[0])) {
            $lastPk = $row[0];
        }
        return $lastPk;
    }

    /**
     * Get last line of file.
     *
     * @param string $filename Path to file.
     * @return string
     */
    private function getLastLine($filename)
    {
        if (!file_exists($filename)) {
            return null;
        }

        if (!$file = @fopen($filename, 'r')) {
            throw new Exception("Unable to open file for reading: $filename");
        }

        $position = -1;
        $line = '';
        $character = '';
        do {
            $line = $character . $line;
            fseek($file, $position--, SEEK_END);
            $character = fgetc($file);
        } while ($character !== false && $character != "\n");

        fclose($file);
        return $line;
    }

    /**
     * Get path to table CSV file.
     *
     * @param string $table Table name.
     * @return string
     */
    private function csvPath($table)
    {
        return "{$this->output_dir}/{$table}.csv";
    }
}
