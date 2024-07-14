create schema DS; 

create table DS.ft_balance_f(
	on_date date not null,
	account_rk numeric not null,
	currency_rk numeric,
	balance_out float,
	primary key (on_date, account_rk)
);

create table DS.ft_posting_f(
	id serial,
	oper_date date not null,
	credit_account_rk numeric not null,
	debet_account_rk numeric not null,
	credit_amount float,
	debet_amount float,
	primary key (id, oper_date, credit_account_rk, debet_account_rk)
);

create table DS.md_account_d(
	data_actual_date date not null,
	data_actual_end_date date not null,
	account_rk numeric not null,
	account_number varchar(20) not null,
	char_type varchar(1) not null,
	currency_rk numeric not null,
	currency_code varchar(3) not null,
	primary key (data_actual_date, account_rk)
);

create table DS.md_currency_d(
	currency_rk numeric not null,
	data_actual_date date not null,
	data_actual_end_date date,
	currency_code varchar(3),
	code_iso_char varchar(3),
	primary key (currency_rk, data_actual_date)
); 

create table DS.md_exchange_rate_d(
	id serial,
	data_actual_date date not null,
	data_actual_end_date date,
	currency_rk numeric not null,
	reduced_cource float,
	code_iso_num varchar(3),
	primary key (id, data_actual_date, currency_rk)
);

create table DS.md_ledger_account_s(
	chapter char(1),
	chapter_name varchar(16),
	section_number int,
	section_name varchar(22),
	subsection_name varchar(21),
	ledger1_account int,
	ledger1_account_name varchar(47),
	ledger_account int not null,
	ledger_account_name varchar(153),
	characteristic char(1),
	is_resident int,
	is_reserve int,
	is_reserved int,
	is_loan int,
	is_reserved_assets int,
	is_overdue int,
	is_interest int,
	pair_account varchar(5),
	start_date date not null,
	end_date date,
	is_rub_only int,
	min_term varchar(1),
	min_term_measure varchar(1),
	max_term varchar(1),
	max_term_measure varchar(1),
	ledger_acc_full_name_translit varchar(1),
	is_revaluation varchar(1),
	is_correct varchar(1),
	primary key (ledger_account, start_date)
); 

create schema LOGS;

create table logs.etl_logs (
	record_id serial,
	start_loading timestamp,
	logging_level varchar(10),
	message text,
	details json,
	end_loading timestamp
);

select * from logs.etl_logs;

create or replace procedure logs.insert_etl_logs (
	i_start_loading timestamp, 
	i_message varchar,
	i_logging_level varchar,
	i_details json
)
language plpgsql
as $$
begin
	
	insert into logs.etl_logs (
		start_loading,
		logging_level,
		message,
		details,		
		end_loading
    )
	values (
		i_start_loading,
		i_logging_level,
		i_message,
		i_details,
		now()
	);

end;$$

select * from ft_balance_f;
select * from ft_posting_f;
select * from md_account_d;
select * from md_currency_d;
select * from md_exchange_rate_d;
select * from md_ledger_account_s;