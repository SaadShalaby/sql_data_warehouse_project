/*
**** Craete DB and Schemas****

purpose : This Code used to create DB ==> "Datawarehouse"
& Bronze , silver and Gold Schemas

*/

use master ;
go

-- create db "Data warehouse"

Create DATABASE Datawarehouse ;
go

-- create schemas Bronze , Silver , Gold 

Create SCHEMA Bronze 
go

Create SCHEMA Silver 
go

Create SCHEMA Gold 
go
