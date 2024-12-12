Alter Table "Date_Dim"
ADD COLUMN "Weekday" Varchar(10);

UPDATE "Date_Dim"
SET "Weekday" = TO_CHAR("Full_Date", 'Day');
