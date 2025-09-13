/*Create database and schemas
Purpose: 
This scrip creates a new database "DataWarehouse" after checking if exists. If dtb exists, its droped and recrested.
It also creates three schemas within the database: bronze, silver, gold.
*/

/*Warning: 
Running this script needs caution as it deletes the database DataWarehouse if it exists.
It should be run once and avoided afterwards as it may lead to dangerous data loss in the process
*/

use master;
go
drop database if exists DataWarehouse;
go

  --- Create the databse DataWarehouse
create database DataWarehouse;
use DataWarehouse;

--- Create schemas for the database
create schema bronze;
go
create schema silver;
go
create schema gold;
