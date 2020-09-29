<?php
require __DIR__ . '/vendor/autoload.php';
$statements = SQLSplitter::splitMySQL(file_get_contents("/code/Scripts/Website_Database_Generator/generate_database.sql"));