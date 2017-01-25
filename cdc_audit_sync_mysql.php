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
    $opt = getopt("a:A:d:eh:u:p:o:v:m:t:?");
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
        if (!$fh = fopen($opt['o'], 'w')) {
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
 * This class is the meat of the script.  It reads the source audit tables
 * and syncs any new rows to the target CSV file.
 */
class CdcAuditSyncMysql
{

    private $host;
    private $user;
    private $pass;
    private $db;

    private $verbosity = 1;
    private $stdout = STDOUT;

    private $output_dir;

    private $tables = null;
    private $wipe = false;

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
     * Executes the engine
     */
    public function run()
    {

        $success = true;
        if ($this->output_dir && $this->output_dir != '=NONE=') {
            $success = $this->syncAuditTables();
        }

        return $success;
    }

   /**
    * Queries mysql information_schema and syncs audit tables to csv files
    */
    private function syncAuditTables()
    {

        try {

            $this->ensureDirExists($this->output_dir);

            // Connect to the MySQL server
            $this->log(sprintf('Connecting to mysql. host = %s, user = %s, pass = %s ', $this->host, $this->user, $this->pass),  __FILE__, __LINE__, LOG_DEBUG);
            $link = @mysql_connect($this->host,$this->user,$this->pass);
            if ($link) {
                $this->log('Connected to mysql.  Getting tables.',  __FILE__, __LINE__, LOG_INFO);

                  // Select the database
                if (!mysql_selectdb($this->db,$link)) {
                    throw new Exception("Unable to select database {$this->db}");
                }

                // Get all tables
                $result = mysql_query('SHOW TABLES');
                while ($row = mysql_fetch_array($result, MYSQL_NUM)) {
                    // Get table name
                    $table = $row[0]  ;

                    if (!strstr($table, '_audit')) {
                        $this->log(sprintf('Found table %s.  Does not appears to be an audit table.  skipping', $table),  __FILE__, __LINE__, LOG_INFO);
                        continue;
                    }

                    if (is_array($this->tables) && !@$this->tables[$table]) {
                        $this->log(sprintf('Found audit table %s.  Not in output list.  skipping', $table),  __FILE__, __LINE__, LOG_INFO);
                        continue;
                    }

                    $this->syncTable($table);
                }

                $this->log(sprintf('Successfully synced audit tables to %s', $this->output_dir),  __FILE__, __LINE__, LOG_WARNING);
            } else {
                throw new Exception("Unable to connect to mysql");
            }
        } catch(Exception $e) {
            $this->log($e->getMessage(), $e->getFile(), $e->getLine(), LOG_ERR);
            return false;
        }
        return true;
    }

    /**
     * Log a message (or not) depending on loglevel
     */
    private function log($msg, $file, $line, $level)
    {
        if ($level >= LOG_DEBUG && $level <= $this->verbosity) {
            fprintf($this->stdout, "%s  -- %s : %s\n", $msg, $file, $line);
        } elseif ($level <= $this->verbosity) {
            fprintf($this->stdout, "%s\n", $msg);
        }
    }

    /**
     * Ensure that given directory exists. throws exception if cannot be created.
     */
    private function ensureDirExists($path)
    {
        $this->log(sprintf('checking if path exists: %s', $path), __FILE__, __LINE__, LOG_DEBUG);
        if (!is_dir($path)) {
            $this->log(sprintf('path does not exist.  creating: %s', $path), __FILE__, __LINE__, LOG_DEBUG);
            $rc = @mkdir($path);
            if (!$rc) {
                throw new Exception("Cannot mkdir " . $path);
            }
            $this->log(sprintf('path created: %s', $path), __FILE__, __LINE__, LOG_INFO);
        }
    }

    /**
     * Syncs audit table to csv file.
     */
    private function syncTable($table)
    {

        $this->log(sprintf("Processing table %s", $table),  __FILE__, __LINE__, LOG_INFO);

        $pk_last = $this->getLatestCsvRowPk($table);
        $result = mysql_query(sprintf('select * from `%s` where audit_pk > %s', $table, $pk_last));

        $mode = $pk_last == -1 ? 'w' : 'a';
        $fh = fopen($this->csvPath($table), $mode);

        if (!$fh) {
            throw new Exception(sprintf("Unable to open file %s for writing", $this->csv_path($table)));
        }

        if ($pk_last == -1) {
            $this->writeCsvHeaderRow($fh, $result);
        }

        while ($row = mysql_fetch_array($result, MYSQL_NUM)) {
            fputcsv($fh, $row);
        }

        fclose($fh);

        if ($this->wipe) {
            $this->wipeAuditTable($table);
        }
    }

    /**
     * Wipes the audit table of all but the last row.
     *
     * Using delete is slow but plays well with concurrent connections.
     * We use an incremental delete to avoid hitting the DB too hard
     * when wiping a large table.
     *
     * truncate plus tmp table for the last record would be faster but I can't
     * find any way to do that atomically without possibility of causing trouble
     * for another session writing to the table.  Same thing for rename.
     *
     * For most applications, if this incremental wipe is performed during each
     * regular sync, then the table should never grow so large that it becomes
     * a major problem.
     *
     * @TODO:  add option to wipe only older than a specific age.
     */
    private function wipeAuditAable($table)
    {

        $this->log(sprintf('wiping audit table: %s', $table), __FILE__, __LINE__, LOG_INFO);

        $incr_amount = 100;

        $loop = 1;
        do {

            if ($loop ++ > 1) {
                sleep(1);
            }

            $result = @mysql_query(sprintf('select count(audit_pk) as cnt, min(audit_pk) as min, max(audit_pk) as max from `%s`', $table));
            $row = @mysql_fetch_assoc($result);

            $cnt = @$row['cnt'];
            $min = @$row['min'];
            $max = @$row['max'];

            if ($cnt <= 1 || !$max) {
                break;
            }

            $delmax = min($min + $incr_amount, $max);
            $this->log(sprintf('wiping audit table rows %s to %s', $min, $delmax), __FILE__, __LINE__, LOG_INFO);

            $query = sprintf('delete from `%s` where audit_pk >= %s and audit_pk < %s', $table, $min, $delmax);
            $result = mysql_query($query);

            if (!$result) {
                throw new Exception(sprintf("mysql error while wiping %s rows.  %s", $incr_amount, $query ));
            }

        } while(true);
    }

    /**
     * given csv fh and mysql result, writes a csv header row with column names
     */
    private function writeCsvHeaderRow($fh, $result)
    {

        $cols = array();
        $i = 0;
        while ($i < mysql_num_fields($result)) {
            $meta = mysql_fetch_field($result, $i);
            $cols[] = $meta->name;
            $i ++;
        }

        fputcsv($fh, $cols);
    }


    /**
     * given source table name, primary key value of latest row in csv file, or -1
     */
    private function getLatestCsvRowPk($table)
    {

        $last_pk = -1;

        $lastline = $this->getLastLine($this->csv_path($table));

        $row = @str_getcsv($lastline);

        $cnt = count($row);

        if ($cnt > 5) {
            $tmp = @$row[ $cnt-1 ];  //audit_pk is always last column.

            if (is_numeric($tmp)) {
                $last_pk = $tmp;
            }
        }
        return $last_pk;
    }

    /**
     * returns the last line of a file, or empty string.
     */
    private function getLastLine($filename)
    {

        if (!file_exists($filename)) {
            return '';
        }

        $fp = @fopen($filename, 'r');

        if (!$fp) {
            throw new Exception(sprintf("Unable to open file %s for reading", $filename));
        }

        $pos = -1; $line = ''; $c = '';
        do {
            $line = $c . $line;
            fseek($fp, $pos--, SEEK_END);
            $c = fgetc($fp);
        } while ($c !== false && $c != "\n");

        fclose($fp);

        return $line;
    }

    /**
     * given source table name, returns audit sql filename
     */
    private function csvPath($table)
    {
        return sprintf("%s/%s.csv", $this->output_dir, $table);
    }

}
