CREATE KEYSPACE IF NOT EXISTS stocks WITH REPLICATION =
{ 'class' : 'SimpleStrategy',
'replication_factor' : '1'
};

CREATE TABLE IF NOT EXISTS stocks.companies (
    company_ID text PRIMARY KEY,
    company_Info text,
    company_Year text,
    company_Month text,
    company_Day text,
    last_update_timestamp timestamp
);

INSERT INTO stocks.companies
    (company_ID, company_Info, company_Year, company_Month, company_Day, last_update_timestamp)
    VALUES ('TEST', 'this is just some text', 'not actual data', 'still not data', 'nope not here either', toTimeStamp(now()));
