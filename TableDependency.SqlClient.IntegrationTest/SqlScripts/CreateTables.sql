IF OBJECT_ID('Columns', 'U') IS NOT NULL DROP TABLE Columns;
GO

CREATE TABLE [dbo].[Columns](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[VarcharColumn] [nvarchar](4000) NOT NULL,
	[DecimalColumn] [decimal](18, 4) NULL,
	[FloatColumn] [float] NULL,
	[NumericColumn] [numeric](18, 2) NULL,
	[CharColumn] [char](10) NULL,
	[DateTime2Column] [datetime2](7) NULL,
	[DatetimeOffsetColumn] [datetimeoffset](7) NULL,
	[TimeColumn] [time](7) NULL
)
GO

IF OBJECT_ID('Customer', 'U') IS NOT NULL DROP TABLE Customer;
GO

CREATE TABLE [Customer](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[First Name] [nvarchar](50) NOT NULL,
	[Second Name] [nvarchar](50) NOT NULL,
	[Born] [datetime] NULL
)
GO

IF OBJECT_ID('NotManagedColumns', 'U') IS NOT NULL DROP TABLE NotManagedColumns;
GO

CREATE TABLE [NotManagedColumns](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[FirstName] [nvarchar](4000) NOT NULL,
	[SecondName] [nvarchar](4000) NOT NULL,
	[ManagedColumnBecauseIsVarcharMAX] [nvarchar](MAX) NOT NULL,
)
GO