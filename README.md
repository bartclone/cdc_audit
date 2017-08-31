cdc_audit - Software to automate change data capture via triggers for MySQL.

cdc_audit presently consists of two scripts:
 - a script to automatically generate MySQL audit tables and triggers.
 - (experimental) a script to auto sync new rows in audit tables to a CSV file.


Features
========

 - Automates generation of audit tables, one audit table per source table.
 - Automates generation of triggers to populate audit tables.
 - Automates syncing of new rows in audit tables to .csv files.
 - Reads MySQL information_schema to automatically determine tables and columns.
 - Fast triggers. only one insert and no selects per trigger execution.
 - Can generate audit tables and triggers for all tables, a specified list of tables, or any not in the list of tables.
 - Can create standard columns for source table values, or use MariaDB dynamic columns for more flexibility.
 - Can sync all audit tables, or a specified list.
 - Retains pre-existing trigger logic, if any, when generating AFTER triggers.
 - Sync script option to delete all but last audit row, to keep source DB small.


Requirements
============

 - PHP 5.6 or greater
 - MySQL 5.1 or greater


Usage
=====

` $ ./cdc_audit_gen_mysql.php`

```
Usage: cdc_audit_gen_mysql.php [Options] -d <db> [-h <host> -d <db> -u <user> -p <pass>]

   Required:
   -d DB              source database

   Options:
   -h HOST            address of machine running mysql.          default = localhost
   -u USER            mysql username.                            default = root
   -p PASS            mysql password.
   -m DIR             path to write audit files.                 default = ./db_audit
   -D DB              destination database for audit tables.     default = value of -d
   -y                 use MariaDB dynamic columns for storing
                      source table values instead of separate
                      columns for each.
   -t TABLES          comma separated list of tables to audit.   default = generate for all tables
   -e                 invert -t, exclude the listed tables.
   -s                 separate triggers, do not rebuild and drop
                      existing triggers (trigger name will be
                      <table>_audit_<event>).
   -A SUFFIX          suffix for audit tables.                   default = '_audit'
   -a PREFIX          prefix for audit tables, replaces suffix.
   -o FILE            send all output to FILE.
   -v <INT>           verbosity level.  default = 4
                        3 = silent except fatal error.
                        4 = silent except warnings.
                        6 = informational.
                        7 = debug.
   -?                 print this help message.
```

` $ ./cdc_audit_sync_mysql.php`

```
Usage: cdc_audit_sync_mysql.php [Options] -d <db> [-h <host> -d <db> -u <user> -p <pass>]

   Required:
   -d DB              database name

   Options:
   -h HOST            address of machine running mysql.          default = localhost
   -u USER            mysql username.                            default = root
   -p PASS            mysql password.
   -m DIR             path to write audit files.                 default = ./cdc_audit_sync
   -t TABLES          comma separated list of tables to audit.   default = generate for all tables
   -e                 invert -t, exclude the listed tables.
   -w                 wipe all but the very last audit row after
                      syncing through truncate and a tmp table.
   -A SUFFIX          suffix for audit tables.                   default = '_audit'
   -a PREFIX          prefix for audit tables, replaces suffix.
   -o FILE            send all output to FILE                    default = send output to STDOUT.
   -v <INT>           verbosity level.  default = 4
                        3 = silent except fatal error.
                        4 = silent except warnings.
                        6 = informational.
                        7 = debug.
   -?                 print this help message.
```


Usage Examples
==============
 To generate audit tables and triggers for all tables in a database:

    php cdc_audit_gen_mysql.php -d <db> [-h <host> -d <db> -u <user> -p <pass>]

 SQL file(s) will be generated in ./cdc_audit_gen.
 They can be applied to your database using the MySQL command-line client, eg:

 $ mysql -u root <database> < ./cdc_audit_gen/table1.sql


 To generate audit tables and triggers for a list of specific tables only:

    php cdc_audit_gen_mysql.php -d <db> -t table1,table2,table3 [-h <host> -d <db> -u <user> -p <pass>]


 To sync all audit tables in a database:

    php cdc_audit_sync_mysql.php -d <db> [-h <host> -d <db> -u <user> -p <pass>]


 To sync two specific audit tables in a database:

    php cdc_audit_sync_mysql.php -d <db> -t table2_audit,table2_audit [-h <host> -d <db> -u <user> -p <pass>]


 Once the sync process is running correctly, the command would typically be
 added to a unix crontab scheduler in order to run it regularly.


Known Issues
==============

 - If you make a change to the source table schema the audit table (unless dynamic
   columns are used) and trigger will not reflect the change.  You will need to
   re-run cdc_audit_gen_mysql to recreate the triggers then if using standard MySQL
   columns manually alter the audit table.

 - no locking is performed on the target CSV file at present.  This could
   cause file corruption.

Todos
=====

 - Use a lockfile to protect .CSV file.  Map-R nfs does not support flock().

 - Check the CSV header row when initiating sync to ensure that # of columns is unchanged and audit_pk column is correct.

 - Auto-Detect schema changes to source table and apply to audit table.

